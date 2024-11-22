use ethers::prelude::*;

abigen!(ZgsFlow, "contracts-abis/Flow.json");

impl Submission {
    pub fn fee(&self, price_per_sector: &U256) -> U256 {
        let mut sectors: u64 = 0;

        // iterate nodes and sum up sectors
        for node in &self.nodes {
            sectors += 1 << node.height.as_u64();
        }

        // sectors multiply by price_per_sector
        U256::from(sectors) * price_per_sector
    }
}
