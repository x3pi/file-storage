// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import "@openzeppelin/contracts-upgradeable@5.4.0/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable@5.4.0/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

contract FileContractRegistry is Initializable, UUPSUpgradeable {
    mapping(address => bool) public isOwner;
    address[] public ownerList;
    mapping(address => uint256) private ownerIndex;
    mapping(address => bool) public validContracts;
    
    // Array to keep track of all registered contracts
    address[] public contractList;
    // Mapping to store the index of each contract in the array for O(1) removal
    mapping(address => uint256) private contractIndex;

    event ContractRegistered(address indexed fileContract);
    event ContractDeregistered(address indexed fileContract);

    modifier onlyOwner() {
        require(isOwner[msg.sender], "Only owner can call this");
        _;
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address _initialOwner) public initializer {
        __UUPSUpgradeable_init();
        
        isOwner[_initialOwner] = true;
        ownerIndex[_initialOwner] = ownerList.length;
        ownerList.push(_initialOwner);
    }

    function _authorizeUpgrade(address newImplementation) internal override onlyOwner {}

    function registerContract(address _fileContract) external onlyOwner {
        require(!validContracts[_fileContract], "Already registered");
        validContracts[_fileContract] = true;
        
        contractIndex[_fileContract] = contractList.length;
        contractList.push(_fileContract);

        emit ContractRegistered(_fileContract);
    }

    function deregisterContract(address _fileContract) external onlyOwner {
        require(validContracts[_fileContract], "Not registered");
        validContracts[_fileContract] = false;

        // O(1) removal: Swap with the last element and pop
        uint256 indexToRemove = contractIndex[_fileContract];
        uint256 lastIndex = contractList.length - 1;
        address lastContract = contractList[lastIndex];

        contractList[indexToRemove] = lastContract;
        contractIndex[lastContract] = indexToRemove;

        contractList.pop();
        delete contractIndex[_fileContract];

        emit ContractDeregistered(_fileContract);
    }

    function isContractValid(address _fileContract) external view returns (bool) {
        return validContracts[_fileContract];
    }

    // --- Owner Management ---

    function addOwner(address _newOwner) external onlyOwner {
        require(!isOwner[_newOwner], "Already an owner");
        require(_newOwner != address(0), "Invalid address");
        
        isOwner[_newOwner] = true;
        ownerIndex[_newOwner] = ownerList.length;
        ownerList.push(_newOwner);
    }

    function removeOwner(address _ownerToRemove) external onlyOwner {
        require(isOwner[_ownerToRemove], "Not an owner");
        require(ownerList.length > 1, "Cannot remove the last owner");
        
        isOwner[_ownerToRemove] = false;
        
        uint256 indexToRemove = ownerIndex[_ownerToRemove];
        uint256 lastIndex = ownerList.length - 1;
        address lastOwner = ownerList[lastIndex];

        ownerList[indexToRemove] = lastOwner;
        ownerIndex[lastOwner] = indexToRemove;

        ownerList.pop();
        delete ownerIndex[_ownerToRemove];
    }

    function getOwners() external view returns (address[] memory) {
        return ownerList;
    }

    // Lấy tổng số lượng contract đã đăng ký
    function getTotalRegisteredContracts() external view returns (uint256) {
        return contractList.length;
    }

    // Lấy danh sách contract có phân trang (pagination)
    function getRegisteredContracts(uint256 offset, uint256 limit) external view returns (address[] memory) {
        uint256 total = contractList.length;
        if (offset >= total) {
            return new address[](0);
        }
        
        uint256 end = offset + limit;
        if (end > total) {
            end = total;
        }
        
        uint256 size = end - offset;
        address[] memory result = new address[](size);
        for (uint256 i = 0; i < size; i++) {
            result[i] = contractList[offset + i];
        }
        
        return result;
    }
}
