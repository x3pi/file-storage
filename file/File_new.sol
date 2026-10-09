// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import "@openzeppelin/contracts-upgradeable@5.4.0/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable@5.4.0/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

// --- CÁC STRUCT & ENUM GIỮ NGUYÊN ---
enum FileStatus {
    Processing,
    Active,
    Deactive,
    Deleted
}

struct Info {
    address owner;
    bytes32 merkleRoot;
    uint64 contentLen;
    uint64 totalChunks;
    uint64 expireTime;
    string name;
    string ext;
    string contentDisposition;
    string contentID;
    FileStatus status;
}

struct FileProgress {
    bytes32 lastChunkHash;
    uint64 processedChunks;
    uint256 processedLength;
}

struct FileInfo {
    Info info;
    FileProgress progress;
    mapping(uint256 => bytes) chunks;
}

struct DownloadSession {
    bytes32 fileKey;
    address user;
    address[] confirmations;
    bool isConfirmed;
}
contract Files is Initializable, UUPSUpgradeable {
    // --- EVENTS ---
    event FileAdded(bytes32 fileKey, string name, uint64 contentLen);
    event ChunkUploaded(bytes32 fileKey, uint256 chunkIndex);
    event FileDeleted(bytes32 fileKey);
    event FileLocked(bytes32 fileKey);
    event FileActivated(address user, bytes32 fileKey);
    event PaymentReceived(
        bytes32 fileKey,
        address payer,
        uint256 amount,
        uint256 downloadCount
    );
    event FundsWithdrawn(address owner, uint256 amount);
    event DownloadKeyGenerated(
        bytes32 downloadKey,
        bytes32 fileKey,
        address user,
        uint256 amount
    );
    event StorageConfirmed(
        bytes32 downloadKey,
        address storageServer,
        uint256 currentConfirmations
    );
    event DownloadKeyConfirmed(bytes32 downloadKey, bytes32 fileKey);
    // --- STORAGE ---
    string[] public rustServerAddresses;

    mapping(bytes32 => FileInfo) public mKeyToFileInfo;
    mapping(string => bytes32) public mNameToFileKey;
    mapping(bytes32 => DownloadSession) public mDownloadKeyToSession;

    // --- File Permissions (Whitelist & Public status) ---
    mapping(bytes32 => address[]) internal _fileWhitelists;
    mapping(bytes32 => mapping(address => bool)) internal _isInWhitelist;
    mapping(bytes32 => bool) public isPublicFile;

    mapping(address => bool) public storageServers;
    address[] public storageServerList;

    address public service; // Biến này chưa thấy dùng, nhưng cứ giữ để đó

    // Role-based access control
    mapping(address => bool) public validators;
    address[] public validatorList;

    mapping(address => bool) public owners;
    address[] public ownerList;

    uint256 public pricePerChunk = 0.0001 ether;
    uint256 private _txCounter;

    // --- State cho Multi-Server Voting ---
    mapping(bytes32 => mapping(address => bool)) public hasVoted;
    mapping(bytes32 => uint256) public fileVotes;
    uint256 internal _requiredVotes;

    // --- MODIFIERS ---
    modifier onlyValidator() {
        require(validators[msg.sender], "Caller is not a validator");
        _;
    }
    modifier onlyOwner() {
        require(owners[msg.sender], "Caller is not owner");
        _;
    }
    modifier onlyOwnerOrValidator() {
        require(
            owners[msg.sender] || validators[msg.sender],
            "Caller is not owner or validator"
        );
        _;
    }

    modifier onlyStorage() {
        require(storageServers[msg.sender], "Caller is not a storage server");
        _;
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize() public initializer {
        __UUPSUpgradeable_init();
        // Logic khởi tạo
        owners[msg.sender] = true;
        ownerList.push(msg.sender);
        pricePerChunk = 0.0001 ether;
    }

    function _authorizeUpgrade(
        address newImplementation
    ) internal override onlyOwner {}

    function setRustServerAddresses(
        string[] memory _addresses
    ) external virtual onlyOwner {
        delete rustServerAddresses;
        for (uint i = 0; i < _addresses.length; i++) {
            rustServerAddresses.push(_addresses[i]);
        }
    }

    function getRustServerAddresses()
        external
        view
        virtual
        returns (string[] memory)
    {
        return rustServerAddresses;
    }

    // --- Owner Management ---

    function addOwner(address _owner) external virtual onlyOwner {
        require(_owner != address(0), "Invalid owner address");
        require(!owners[_owner], "Address is already an owner");
        owners[_owner] = true;
        ownerList.push(_owner);
    }

    function removeOwner(address _owner) external virtual onlyOwner {
        require(owners[_owner], "Address is not an owner");
        require(_owner != msg.sender, "Cannot remove yourself"); // Logic business
        owners[_owner] = false;

        for (uint256 i = 0; i < ownerList.length; i++) {
            if (ownerList[i] == _owner) {
                ownerList[i] = ownerList[ownerList.length - 1];
                ownerList.pop();
                break;
            }
        }
    }

    function isOwner(address _address) external view virtual returns (bool) {
        return owners[_address];
    }

    function getOwnerList() external view virtual returns (address[] memory) {
        return ownerList;
    }

    // --- Validator Management ---

    function addValidator(address _validator) external virtual onlyOwner {
        require(_validator != address(0), "Invalid validator address");
        require(!validators[_validator], "Address is already a validator");
        validators[_validator] = true;
        validatorList.push(_validator);
    }

    function removeValidator(address _validator) external virtual onlyOwner {
        require(validators[_validator], "Address is not a validator");
        validators[_validator] = false;

        for (uint256 i = 0; i < validatorList.length; i++) {
            if (validatorList[i] == _validator) {
                validatorList[i] = validatorList[validatorList.length - 1];
                validatorList.pop();
                break;
            }
        }
    }

    function isValidator(
        address _address
    ) external view virtual returns (bool) {
        return validators[_address];
    }

    function getValidatorList()
        external
        view
        virtual
        returns (address[] memory)
    {
        return validatorList;
    }

    // --- Storage Server Management ---

    function addStorageServer(address _server) external virtual onlyOwner {
        require(_server != address(0), "Invalid server address");
        require(
            !storageServers[_server],
            "Address is already a storage server"
        );
        storageServers[_server] = true;
        storageServerList.push(_server);
    }

    function removeStorageServer(address _server) external virtual onlyOwner {
        require(storageServers[_server], "Address is not a storage server");
        storageServers[_server] = false;

        for (uint256 i = 0; i < storageServerList.length; i++) {
            if (storageServerList[i] == _server) {
                storageServerList[i] = storageServerList[
                    storageServerList.length - 1
                ];
                storageServerList.pop();
                break;
            }
        }
    }

    function isStorageServer(
        address _address
    ) external view virtual returns (bool) {
        return storageServers[_address];
    }

    function getStorageServerList()
        external
        view
        virtual
        returns (address[] memory)
    {
        return storageServerList;
    }

    function setPricePerChunk(uint256 _newPrice) external virtual onlyOwner {
        pricePerChunk = _newPrice;
    }

    function calculatePrice(
        uint256 numChunks
    ) public view virtual returns (uint256) {
        return numChunks * pricePerChunk;
    }

    function pushFileInfo(
        Info memory info
    ) public payable virtual returns (bytes32 fileKey) {
        require(
            info.expireTime > block.timestamp + 1 days,
            "Expire time error"
        );

        uint256 requiredPayment = calculatePrice(info.totalChunks);
        require(msg.value >= requiredPayment, "Insufficient payment");

        _txCounter++;
        fileKey = keccak256(
            abi.encodePacked(
                address(this),
                msg.sender,
                info.contentLen,
                info.expireTime,
                info.merkleRoot,
                info.name,
                info.ext,
                block.timestamp,
                _txCounter
            )
        );

        mNameToFileKey[info.name] = fileKey;
        require(
            mKeyToFileInfo[fileKey].info.merkleRoot == bytes32(0),
            "File exists"
        );

        mKeyToFileInfo[fileKey].info = Info({
            owner: msg.sender,
            merkleRoot: info.merkleRoot,
            contentLen: info.contentLen,
            totalChunks: info.totalChunks,
            expireTime: info.expireTime,
            name: info.name,
            ext: info.ext,
            status: FileStatus.Processing,
            contentDisposition: info.contentDisposition,
            contentID: info.contentID
        });

        mKeyToFileInfo[fileKey].progress = FileProgress({
            lastChunkHash: bytes32(0),
            processedChunks: 0,
            processedLength: 0
        });

        emit FileAdded(fileKey, info.name, info.contentLen);
        if (msg.value > requiredPayment) {
            uint256 refundAmount = msg.value - requiredPayment;
            (bool success, ) = payable(msg.sender).call{value: refundAmount}(
                ""
            );
            require(success, "Refund failed");
        }
        return fileKey;
    }

    function getFileKeyFromName(
        string[] memory names
    ) external view virtual returns (bytes32[] memory) {
        bytes32[] memory filekeys = new bytes32[](names.length);
        for (uint256 i; i < names.length; i++) {
            filekeys[i] = mNameToFileKey[names[i]];
        }
        return filekeys;
    }

    function uploadChunk(
        bytes32 fileKey,
        bytes memory chunkData,
        uint256 chunkIndex,
        bytes32[] memory merkleProof
    ) public virtual {
        // Logic upload chunk (bạn chưa viết logic ở code gốc, nhưng cứ để virtual)
    }

    function deleteFile(bytes32 fileKey) external virtual {
        FileInfo storage file = mKeyToFileInfo[fileKey];
        require(file.info.owner == msg.sender, "Caller is not the owner");
        require(file.info.status != FileStatus.Deleted, "File already deleted");

        delete mNameToFileKey[file.info.name];
        file.info.status = FileStatus.Deleted;

        for (uint256 i = 0; i < file.info.totalChunks; i++) {
            delete file.chunks[i];
        }
        delete file.progress;
        emit FileDeleted(fileKey);
    }

    function renewTime(
        bytes32 fileKey,
        uint64 _newExpireTime
    ) external virtual {
        FileInfo storage file = mKeyToFileInfo[fileKey];
        require(file.info.owner == msg.sender, "Caller is not the owner");
        require(file.info.status != FileStatus.Deleted, "Deleted");
        require(_newExpireTime > block.timestamp + 1 days, "Time error");
        file.info.expireTime = _newExpireTime;
    }

    function getFileInfo(
        bytes32 fileKey
    ) external view virtual returns (Info memory) {
        return mKeyToFileInfo[fileKey].info;
    }

    function getFilesInfo(
        bytes32[] memory fileKeys
    ) external view virtual returns (Info[] memory infos) {
        infos = new Info[](fileKeys.length);
        for (uint256 i = 0; i < fileKeys.length; i++) {
            infos[i] = mKeyToFileInfo[fileKeys[i]].info;
        }
    }

    function getFileProgress(
        bytes32 fileKey
    ) external view virtual returns (FileProgress memory) {
        FileInfo storage file = mKeyToFileInfo[fileKey];
        require(file.info.status == FileStatus.Processing, "Not exists");
        return mKeyToFileInfo[fileKey].progress;
    }

    function downloadFile(
        bytes32 fileKey,
        uint256 start,
        uint256 limit
    ) public virtual {
        // Logic download
    }

    function confirmFileActive(bytes32 fileKey) external virtual {
        FileInfo storage file = mKeyToFileInfo[fileKey];
        require(file.info.status == FileStatus.Processing, "Not processing");
        file.info.status = FileStatus.Active;
        emit FileActivated(file.info.owner, fileKey);
    }

    // HÀM QUAN TRỌNG: Logic thanh toán rất hay thay đổi
    function payForDownload(
        bytes32 fileKey,
        uint256 downloadTimes
    ) external payable virtual {
        require(downloadTimes > 0, "Times > 0");
        FileInfo storage file = mKeyToFileInfo[fileKey];
        require(file.info.status == FileStatus.Active, "Not active");
        require(block.timestamp <= file.info.expireTime, "Expired");

        uint256 downloadFee = calculatePrice(file.info.totalChunks) *
            downloadTimes;
        require(msg.value >= downloadFee, "Insufficient payment");

        _txCounter++;
        bytes32 downloadKey = keccak256(
            abi.encodePacked(address(this), fileKey, msg.sender, block.timestamp, _txCounter)
        );

        mDownloadKeyToSession[downloadKey] = DownloadSession({
            fileKey: fileKey,
            user: msg.sender,
            confirmations: new address[](0),
            isConfirmed: false
        });

        emit DownloadKeyGenerated(downloadKey, fileKey, msg.sender, msg.value);
    }

    function confirmServerDownload(
        bytes32 downloadKey
    ) external virtual onlyStorage {
        DownloadSession storage session = mDownloadKeyToSession[downloadKey];
        require(session.fileKey != bytes32(0), "Invalid key");
        require(!session.isConfirmed, "Confirmed already");

        for (uint i = 0; i < session.confirmations.length; i++) {
            require(
                session.confirmations[i] != msg.sender,
                "Already confirmed"
            );
        }
        session.confirmations.push(msg.sender);
        if (session.confirmations.length >= storageServerList.length) {
            session.isConfirmed = true;
            emit DownloadKeyConfirmed(downloadKey, session.fileKey);
        }
    }

    function withdrawAmount(uint256 amount) external virtual onlyOwner {
        require(amount > 0, "Zero amount");
        require(address(this).balance >= amount, "Insufficient");
        (bool success, ) = payable(msg.sender).call{value: amount}("");
        require(success, "Failed");
        emit FundsWithdrawn(msg.sender, amount);
    }

    function getContractBalance() external view virtual returns (uint256) {
        return address(this).balance;
    }

    function getDownloadSessionInfo(
        bytes32 downloadKey
    ) external view virtual returns (DownloadSession memory) {
        return mDownloadKeyToSession[downloadKey];
    }

    // --- CÁC HÀM MỚI TỪ FileV2 ---

    function setPublicStatus(bytes32 fileKey, bool status) public virtual {
        require(
            mKeyToFileInfo[fileKey].info.owner == msg.sender,
            "Not file owner"
        );
        isPublicFile[fileKey] = status;
    }

    function addWhitelist(
        bytes32 fileKey,
        address[] calldata users
    ) public virtual {
        require(
            mKeyToFileInfo[fileKey].info.owner == msg.sender,
            "Not file owner"
        );
        address[] storage currentList = _fileWhitelists[fileKey];
        for (uint256 i = 0; i < users.length; i++) {
            if (!_isInWhitelist[fileKey][users[i]] && users[i] != address(0)) {
                _isInWhitelist[fileKey][users[i]] = true;
                currentList.push(users[i]);
            }
        }
    }

    function getWhitelist(
        bytes32 fileKey
    ) external view virtual returns (address[] memory) {
        return _fileWhitelists[fileKey];
    }

    function setRequiredVotes(uint256 newRequiredVotes) external onlyOwner {
        require(newRequiredVotes > 0, "Required votes must be > 0");
        _requiredVotes = newRequiredVotes;
    }

    function getRequiredVotes() public view returns (uint256) {
        return _requiredVotes == 0 ? 2 : _requiredVotes;
    }

    function removeWhitelist(
        bytes32 fileKey,
        address[] calldata users
    ) external virtual {
        require(
            mKeyToFileInfo[fileKey].info.owner == msg.sender,
            "Not file owner"
        );
        address[] storage currentList = _fileWhitelists[fileKey];
        for (uint256 i = 0; i < users.length; i++) {
            address userToRemove = users[i];
            if (_isInWhitelist[fileKey][userToRemove]) {
                _isInWhitelist[fileKey][userToRemove] = false;
                // Remove from array (swap with last and pop)
                for (uint256 j = 0; j < currentList.length; j++) {
                    if (currentList[j] == userToRemove) {
                        currentList[j] = currentList[currentList.length - 1];
                        currentList.pop();
                        break;
                    }
                }
            }
        }
    }

    function confirmServerUploadBatch(bytes32[] calldata fileKeys) external virtual onlyStorage {
        for (uint256 i = 0; i < fileKeys.length; i++) {
            bytes32 fileKey = fileKeys[i];
            FileInfo storage file = mKeyToFileInfo[fileKey];
            if (file.info.status == FileStatus.Processing) {
                // Kiểm tra xem server này đã vote chưa
                if (!hasVoted[fileKey][msg.sender]) {
                    hasVoted[fileKey][msg.sender] = true;
                    fileVotes[fileKey] += 1;
                    
                    // Nếu file có > 1 chunk thì cần getRequiredVotes() vote
                    // Nếu file có 1 chunk thì chỉ cần 1 vote (từ server chẵn)
                    uint256 requiredVotes = file.info.totalChunks > 1 ? getRequiredVotes() : 1;
                    
                    if (fileVotes[fileKey] >= requiredVotes) {
                        file.info.status = FileStatus.Active;
                        emit FileActivated(file.info.owner, fileKey);
                    }
                }
            }
        }
    }
}
