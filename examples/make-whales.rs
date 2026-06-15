use rand::RngExt;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    native_token::LAMPORTS_PER_SOL,
    program_pack::Pack,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Welcome! Make whale transfers here.");

    let rpc_client = solana_client::rpc_client::RpcClient::new("http://localhost:8899");

    // create a user keypair, and a mint keypair
    let user_key = generate_airdropped_keypair(&rpc_client)?;
    let mint_key = Keypair::new();

    // create a mint
    let mint_space = spl_token_interface::state::Mint::LEN;
    let rent_exempt = rpc_client.get_minimum_balance_for_rent_exemption(mint_space)?;

    let create_acc_ix = solana_system_interface::instruction::create_account(
        &user_key.pubkey(),
        &mint_key.pubkey(),
        rent_exempt,
        mint_space as u64,
        &spl_token_interface::id(),
    );
    let init_mint_ix = spl_token_interface::instruction::initialize_mint2(
        &spl_token_interface::ID,
        &mint_key.pubkey(),
        &user_key.pubkey(),
        None,
        6,
    )?;

    let latest_blockhash = rpc_client.get_latest_blockhash()?;
    let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[create_acc_ix, init_mint_ix],
        Some(&user_key.pubkey()),
        &[&user_key, &mint_key],
        latest_blockhash,
    );

    let tx_sig = rpc_client.send_and_confirm_transaction(&tx)?;
    println!("Mint initialized Successfully - {}...", tx_sig.to_string().get(..10).unwrap());

    //make whale transfer
    loop {
        make_whales(&rpc_client, &mint_key.pubkey(), &user_key)?;
    }
}

/// generates a random keypair, and airdrop it
fn generate_airdropped_keypair(
    rpc_client: &RpcClient,
) -> Result<Keypair, Box<dyn std::error::Error>> {
    let keypair = Keypair::new();
    // add lamports to user_key
    let airdrop_tx = rpc_client.request_airdrop(&keypair.pubkey(), 100 * LAMPORTS_PER_SOL)?;
    loop {
        let confirmed = rpc_client.confirm_transaction(&airdrop_tx)?;
        let balance = rpc_client.get_balance(&keypair.pubkey())?;
        if confirmed && balance > 0 {
            break;
        }
    }
    Ok(keypair)
}

/// token minted to wallet, and return the ata
fn mint_to(
    rpc_client: &RpcClient,
    mint: &Pubkey,
    mint_authority: &Keypair,
    wallet: &Pubkey,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let ata =
        spl_associated_token_account_interface::address::get_associated_token_address(wallet, mint);

    // create ata if not already exists
    match rpc_client.get_account(&ata) {
        Ok(_) => (),
        Err(_) => {
            // create ata
            let create_ata_ix = spl_associated_token_account_interface::instruction::create_associated_token_account(&mint_authority.pubkey(), wallet, mint, &spl_token_interface::id());
            let latest_blockhash = rpc_client.get_latest_blockhash()?;
            let txn = solana_sdk::transaction::Transaction::new_signed_with_payer(
                &[create_ata_ix],
                Some(&mint_authority.pubkey()),
                &[mint_authority],
                latest_blockhash,
            );
            let txn_sig = rpc_client.send_and_confirm_transaction(&txn)?;
            println!("Ata created: {}...", txn_sig.to_string().get(..7).unwrap());

            ()
        }
    }

    let mint_to_ix = spl_token_interface::instruction::mint_to(
        &spl_token_interface::id(),
        mint,
        &ata,
        &mint_authority.pubkey(),
        &[&mint_authority.pubkey()],
        1_000_000 * 1_000_000,
    )?;

    let latest_blockhash = rpc_client.get_latest_blockhash()?;
    let txn = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[mint_to_ix],
        Some(&mint_authority.pubkey()),
        &[mint_authority],
        latest_blockhash,
    );

    let txn_sig = rpc_client.send_and_confirm_transaction(&txn)?;
    println!("Minted tokens: {}...", txn_sig.to_string().get(..7).unwrap());

    Ok(ata)
}

/// genereates random user, airdropped it and make whale transafers
fn make_whales(
    rpc_client: &RpcClient,
    mint: &Pubkey,
    mint_authority: &Keypair,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_wallet = generate_airdropped_keypair(rpc_client)?;
    let dest_wallet = generate_airdropped_keypair(rpc_client)?;

    let source_ata = mint_to(rpc_client, mint, mint_authority, &source_wallet.pubkey())?;
    let dest_ata = mint_to(rpc_client, mint, mint_authority, &dest_wallet.pubkey())?;

    let amount: u64 = rand::rng().random_range(100_000..=500_000);
    let transfer_ix = spl_token_interface::instruction::transfer_checked(
        &spl_token_interface::ID,
        &source_ata,
        mint,
        &dest_ata,
        &source_wallet.pubkey(),
        &[&source_wallet.pubkey()],
        amount,
        6,
    )?;

    let latest_blockhash = rpc_client.get_latest_blockhash()?;
    let txn = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[transfer_ix],
        Some(&source_wallet.pubkey()),
        &[source_wallet],
        latest_blockhash,
    );

    let txn_sig = rpc_client.send_and_confirm_transaction(&txn)?;
    println!(
        "Whale transfer successfull: {}...",
        txn_sig.to_string().get(..10).unwrap()
    );

    Ok(())
}
