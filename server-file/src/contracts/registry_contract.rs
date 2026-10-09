use alloy::sol;

sol!(
    #[sol(rpc)]
    interface Registry {
        function isContractValid(address _contract) external view returns (bool);
        function getRegisteredContracts(uint256 offset, uint256 limit) external view returns (address[] memory);
        function getTotalRegisteredContracts() external view returns (uint256);
    }
);
