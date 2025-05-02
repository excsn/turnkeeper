/// Macro to simplify creating a `BoxedExecFn` compatible closure.
///
/// Takes an optional synchronous setup block and a mandatory async logic block.
/// Handles the necessary boxing (`Box::new`, `Box::pin`).
///
/// # Usage
///
/// ```ignore
/// # use turnkeeper::turnkey_job_fn; // Assuming macro is re-exported or path adjusted
/// # use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
/// # use std::time::Duration;
/// # let counter = Arc::new(AtomicUsize::new(0));
/// # let message = "Hello".to_string();
/// // With setup block:
/// let job_fn_1 = turnkey_job_fn! {
///     // Optional setup block runs immediately when outer closure is created.
///     {
///         let job_counter = counter.clone(); // Clone Arcs here
///         let local_msg = message;         // Move owned data here
///         println!("Setup block executed");
///     }
///     // Main logic block (implicitly wrapped in `async move`)
///     // Captures variables defined in the setup block.
///     {
///         let count = job_counter.fetch_add(1, Ordering::SeqCst) + 1;
///         println!("Job executing (Count: {}): {}", count, local_msg);
///         tokio::time::sleep(Duration::from_millis(10)).await;
///         true // Must evaluate to bool
///     }
/// };
///
/// // Without setup block:
/// let job_fn_2 = turnkey_job_fn! {
///     // Main logic block only
///     {
///         println!("Simple job executing");
///         tokio::time::sleep(Duration::from_millis(5)).await;
///         true
///     }
/// };
/// # let scheduler: turnkeeper::TurnKeeper = todo!(); // Placeholder for example
/// # let request: turnkeeper::job::TKJobRequest = todo!(); // Placeholder
/// # tokio_test::block_on(scheduler.add_job_async(request.clone(), job_fn_1));
/// # tokio_test::block_on(scheduler.add_job_async(request, job_fn_2));
/// ```
#[macro_export]
macro_rules! job_fn {
    // Matcher 1: Optional setup block `{...}` followed by the main logic block `{...}`
    (
        { $($setup_stmts:stmt);* $(;)? } // Setup block (optional contents)
        $main_block:block                 // Main logic block
    ) => {
        move || {
            // Execute setup statements
            $($setup_stmts)*

            let fut = async move { $main_block };

            Box::pin(fut) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'static>>
        }
    };

    // Matcher 2: Only the main logic block is provided
    (
        $main_block:block // Main logic block
    ) => {
         move || {
            let fut = async move { $main_block };

            Box::pin(fut) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'static>>
        }
    };
}