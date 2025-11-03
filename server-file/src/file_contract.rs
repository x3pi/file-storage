use alloy::sol;

sol!(
    #[sol(rpc)]
    contract Files {
        // --- Enums ---
        // Định nghĩa enum tương ứng với Solidity để dễ sử dụng hơn.
        // Trong ABI, nó vẫn sẽ được coi là uint8.
        #[derive(Debug)]
        enum FileStatus {
            Processing, // 0
            Active,     // 1
            Deactive,   // 2
            Deleted     // 3
        }

        // --- Structs ---

        // Sửa lại struct này cho khớp với struct `Info` trong Solidity.
        // Tên struct đã được đổi thành `Info` cho nhất quán.
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
            FileStatus status; // Sử dụng enum đã định nghĩa ở trên (vẫn là uint8 trong ABI)
        }

        // Sửa lại struct này cho khớp với struct `DownloadSession` trong Solidity.
        // Tên struct đã được đổi thành `DownloadSession` cho nhất quán.
        struct DownloadSession {
            bytes32 fileKey;
            address user;
            address[] confirmations; // Thêm trường bị thiếu
            bool isConfirmed;        // Thêm trường bị thiếu
        }

        // --- Events ---
        event FileActivated(address user, bytes32 fileKey);
        event DownloadKeyConfirmed(bytes32 downloadKey, bytes32 fileKey);
        // --- Functions ---

        // Hàm này đã đúng
        function confirmServerDownload(bytes32 downloadKey) external;

        // Sửa kiểu trả về của hàm này để sử dụng struct `Info` đã được định nghĩa lại ở trên
        function getFileInfo(bytes32 fileKey) external view returns (Info memory);

        // Sửa kiểu trả về của hàm này để sử dụng struct `DownloadSession` đã được định nghĩa lại ở trên
        function getDownloadSessionInfo(bytes32 downloadKey) external view returns (DownloadSession memory);
    }
);