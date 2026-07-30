use alloy::sol;

sol!(
    #[sol(rpc)]
    interface Registry {
        function isContractValid(address _contract) external view returns (bool);
    }
);
