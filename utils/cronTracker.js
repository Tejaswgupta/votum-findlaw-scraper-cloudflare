/**
 * Utility module for tracking cron job execution in Supabase.
 */

/**
 * Start tracking a cron job run.
 * 
 * @param {Object} supabaseClient - Initialized Supabase client
 * @param {string} jobName - Name of the job (e.g., "us_caselaw_scraper")
 * @returns {Promise<Object>} - Object containing { runId, success }
 */
export async function startJob(supabaseClient, jobName) {
    try {
        const { data, error } = await supabaseClient
            .from('cron_job_runs')
            .insert([{
                job_name: jobName,
                start_time: new Date().toISOString(),
                status: 'started',
                new_cases_found: 0,
                pages_processed: 0
            }])
            .select('id')
            .single();

        if (error) {
            console.error(`Error starting cron job tracking: ${error.message}`);
            return { runId: null, success: false };
        }

        console.log(`Cron job '${jobName}' started. Run ID: ${data.id}`);
        return { runId: data.id, success: true };
    } catch (e) {
        console.error(`Exception in cron job tracking: ${e.message}`);
        return { runId: null, success: false };
    }
}

/**
 * Mark a job as completed with metrics.
 * 
 * @param {Object} supabaseClient - Initialized Supabase client
 * @param {string} runId - ID of the job run from startJob
 * @param {Object} metrics - Dictionary of metrics (e.g., {newCasesFound: 10, pagesProcessed: 5})
 * @returns {Promise<boolean>} - Success status
 */
export async function completeJob(supabaseClient, runId, metrics) {
    if (!runId) {
        console.warn('Warning: Cannot complete job, no runId provided.');
        return false;
    }

    try {
        const updateData = {
            end_time: new Date().toISOString(),
            status: 'completed',
            ...metrics
        };

        const { error } = await supabaseClient
            .from('cron_job_runs')
            .update(updateData)
            .eq('id', runId);

        if (error) {
            console.error(`Error completing cron job: ${error.message}`);
            return false;
        }

        return true;
    } catch (e) {
        console.error(`Exception completing cron job: ${e.message}`);
        return false;
    }
}

/**
 * Mark a job as failed with error details.
 * 
 * @param {Object} supabaseClient - Initialized Supabase client
 * @param {string} runId - ID of the job run from startJob
 * @param {Object} metrics - Dictionary of metrics (e.g., {newCasesFound: 10, pagesProcessed: 5})
 * @param {string} errorMessage - Description of the error that caused the failure
 * @returns {Promise<boolean>} - Success status
 */
export async function failJob(supabaseClient, runId, metrics, errorMessage) {
    if (!runId) {
        console.warn('Warning: Cannot fail job, no runId provided.');
        return false;
    }

    try {
        const updateData = {
            end_time: new Date().toISOString(),
            status: 'failed',
            error_message: errorMessage,
            ...metrics
        };

        const { error } = await supabaseClient
            .from('cron_job_runs')
            .update(updateData)
            .eq('id', runId);

        if (error) {
            console.error(`Error marking cron job as failed: ${error.message}`);
            return false;
        }

        return true;
    } catch (e) {
        console.error(`Exception marking cron job as failed: ${e.message}`);
        return false;
    }
} 