-- Simple Supabase Security (Just the essentials)

-- 1. Enable RLS on your main tables
ALTER TABLE calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE call_participants ENABLE ROW LEVEL SECURITY;

-- 2. Allow your service to access the data
CREATE POLICY "Service can access all data" ON calls
    FOR ALL USING (true);

CREATE POLICY "Service can access all data" ON call_participants
    FOR ALL USING (true);

-- 3. Create a simple audit log (optional)
CREATE TABLE IF NOT EXISTS simple_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action TEXT,
    table_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Simple function to log important events
CREATE OR REPLACE FUNCTION log_event(action_name TEXT, table_name TEXT)
RETURNS VOID AS $$
BEGIN
    INSERT INTO simple_audit (action, table_name) VALUES (action_name, table_name);
END;
$$ LANGUAGE plpgsql; 