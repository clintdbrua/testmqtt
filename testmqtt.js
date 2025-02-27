//https://github.com/clintdbrua/mqtttosql.git

// Import modules
const mqtt = require('mqtt');
const sql = require('mssql');

// Debugging
console.log("Starting MQTT to SQL script...");

// MQTT Broker Connection
const client = mqtt.connect('mqtt://localhost', { // Replace with your actual broker IP
  username: 'clintb',
  password: 'Hellohello',
});

client.on('connect', () => {
    console.log("Connected to MQTT broker.");
    client.subscribe('lincoln_electric/welding/100002021218015/summary', (err) => {
        if (err) {
            console.error("Subscription error:", err);
        } else {
            console.log("Subscribed to topic: lincoln_electric/welding/100002021218015/summary");
        }
    });
});

// Database configuration
const dbConfig = {
    user: 'webuser',
    password: 'Webpassword01!',
    server: '192.168.2.11', // Change this
    database: 'MfgProduction',
    options: {
        encrypt: false, // Set to true if using Azure SQL
        trustServerCertificate: true
    }
};

// Create a pool of connections
let pool;

// Function to connect to the SQL database using pooling
async function connectToDatabase() {
    try {
        pool = await sql.connect(dbConfig);
        console.log("Connected to SQL database.");
    } catch (err) {
        console.error("Database connection error:", err);
    }
}

// Connect to the database initially
connectToDatabase();

// Reconnect logic for pool (to ensure the pool is always available)
async function ensureDatabaseConnection() {
    if (!pool || pool.closed) {
        console.log('Reconnecting to SQL database...');
        await connectToDatabase();
    }
}

client.on('message', async (topic, message) => {
    try {
        // Convert message to JSON
        const data = JSON.parse(message.toString());
        console.log(`Received message on topic: ${topic}`, data);

        // Ensure the database connection is active before proceeding
        await ensureDatabaseConnection();

        // Insert Data into SQL Table
        const query = `
            INSERT INTO WeldSummary (
                timestamp, weld_record_index, 
                current_average, current_min, current_max, current_pct_high, current_pct_low, 
                current_limit_high, current_limit_low, 
                voltage_average, voltage_min, voltage_max, voltage_pct_high, voltage_pct_low, 
                voltage_limit_high, voltage_limit_low, 
                wire_feed_speed_average, wire_feed_speed_min, wire_feed_speed_max, 
                wire_feed_speed_pct_high, wire_feed_speed_pct_low, 
                wire_feed_speed_limit_high, wire_feed_speed_limit_low, 
                using_weld_score, start_delay, end_delay, 
                duration_value, duration_limit_high, duration_limit_low, 
                consumable_density, consumable_diameter, 
                true_energy, weld_profile, weld_start_time, 
                status_current_low, status_current_high, 
                status_voltage_low, status_voltage_high, 
                status_wire_feed_speed_low, status_wire_feed_speed_high, 
                status_weld_score_low, status_arc_time_out_of_limits, 
                status_short_weld, status_arc_time_low, status_arc_time_high, 
                status_alarm, status_latch_alarm, status_fault, status_latch_fault, 
                limits_enabled_arc_time, limits_enabled_weld_score, 
                limits_enabled_wire_feed_speed, limits_enabled_current, limits_enabled_voltage, 
                part_serial, operator_id, consumable_lot, 
                weld_mode, assembly_id, seam_id, 
                average_motor_current, average_gas_flow, 
                warnings_wire_feed_speed_high, warnings_wire_feed_speed_low, 
                warnings_voltage_high, warnings_voltage_low, 
                warnings_current_high, warnings_current_low, 
                warnings_weld_score, warnings_time, 
                weld_type, wire_drive_sn
            ) VALUES (
                @timestamp, @weld_record_index, 
                @current_average, @current_min, @current_max, @current_pct_high, @current_pct_low, 
                @current_limit_high, @current_limit_low, 
                @voltage_average, @voltage_min, @voltage_max, @voltage_pct_high, @voltage_pct_low, 
                @voltage_limit_high, @voltage_limit_low, 
                @wire_feed_speed_average, @wire_feed_speed_min, @wire_feed_speed_max, 
                @wire_feed_speed_pct_high, @wire_feed_speed_pct_low, 
                @wire_feed_speed_limit_high, @wire_feed_speed_limit_low, 
                @using_weld_score, @start_delay, @end_delay, 
                @duration_value, @duration_limit_high, @duration_limit_low, 
                @consumable_density, @consumable_diameter, 
                @true_energy, @weld_profile, @weld_start_time, 
                @status_current_low, @status_current_high, 
                @status_voltage_low, @status_voltage_high, 
                @status_wire_feed_speed_low, @status_wire_feed_speed_high, 
                @status_weld_score_low, @status_arc_time_out_of_limits, 
                @status_short_weld, @status_arc_time_low, @status_arc_time_high, 
                @status_alarm, @status_latch_alarm, @status_fault, @status_latch_fault, 
                @limits_enabled_arc_time, @limits_enabled_weld_score, 
                @limits_enabled_wire_feed_speed, @limits_enabled_current, @limits_enabled_voltage, 
                @part_serial, @operator_id, @consumable_lot, 
                @weld_mode, @assembly_id, @seam_id, 
                @average_motor_current, @average_gas_flow, 
                @warnings_wire_feed_speed_high, @warnings_wire_feed_speed_low, 
                @warnings_voltage_high, @warnings_voltage_low, 
                @warnings_current_high, @warnings_current_low, 
                @warnings_weld_score, @warnings_time, 
                @weld_type, @wire_drive_sn
            )
        `;

            // Execute the query
        await pool.request()
            .input('timestamp', sql.BigInt, data.timestamp)
            .input('weld_record_index', sql.Int, data.weld_record_index)
            .input('current_average', sql.Float, data.current.average)
            .input('current_min', sql.Float, data.current.min)
            .input('current_max', sql.Float, data.current.max)
            .input('current_pct_high', sql.Int, data.current.pct_high)
            .input('current_pct_low', sql.Int, data.current.pct_low)
            .input('current_limit_high', sql.Float, data.current.limits.high)
            .input('current_limit_low', sql.Float, data.current.limits.low)
            .input('voltage_average', sql.Float, data.voltage.average)
            .input('voltage_min', sql.Float, data.voltage.min)
            .input('voltage_max', sql.Float, data.voltage.max)
            .input('voltage_pct_high', sql.Int, data.voltage.pct_high)
            .input('voltage_pct_low', sql.Int, data.voltage.pct_low)
            .input('voltage_limit_high', sql.Float, data.voltage.limits.high)
            .input('voltage_limit_low', sql.Float, data.voltage.limits.low)
            .input('wire_feed_speed_average', sql.Int, data.wire_feed_speed.average)
            .input('wire_feed_speed_min', sql.Int, data.wire_feed_speed.min)
            .input('wire_feed_speed_max', sql.Int, data.wire_feed_speed.max)
            .input('wire_feed_speed_pct_high', sql.Int, data.wire_feed_speed.pct_high)
            .input('wire_feed_speed_pct_low', sql.Int, data.wire_feed_speed.pct_low)
            .input('wire_feed_speed_limit_high', sql.Int, data.wire_feed_speed.limits.high)
            .input('wire_feed_speed_limit_low', sql.Int, data.wire_feed_speed.limits.low)
            .input('using_weld_score', sql.Bit, data.using_weld_score)
            .input('weld_type', sql.NVarChar, data.weld_type)
            .input('wire_drive_sn', sql.NVarChar, data.wire_drive_sn)
            .input('start_delay', sql.Float, data.start_delay)
            .input('end_delay', sql.Float, data.end_delay)
           // .input('duration_value', sql.Float, data.duration_value)  // Old Input Value
            .input('duration_value', sql.Float, data.duration.value)
            .input('duration_limit_high', sql.Float, data.duration.limit.high)  // Changed this to data.duration.limit_high from data.duration_limit.high
            .input('duration_limit_low', sql.Float, data.duration.limit.low)    // Changed this to data.duration.limit_low from data.duration_limit.low
            .input('consumable_density', sql.Float, data.consumable.density)
            .input('consumable_diameter', sql.Float, data.consumable.diameter)
            .input('true_energy', sql.Float, data.true_energy)
            .input('weld_profile', sql.Int, data.weld_profile)
            .input('weld_start_time', sql.DateTime, new Date(data.weld_start_time * 1000))
            .input('status_current_low', sql.Bit, data.status.current_low)
            .input('status_current_high', sql.Bit, data.status.current_high)
            .input('status_voltage_low', sql.Bit, data.status.voltage_low)
            .input('status_voltage_high', sql.Bit, data.status.voltage_high)
            .input('status_wire_feed_speed_low', sql.Bit, data.status.wire_feed_speed_low)
            .input('status_wire_feed_speed_high', sql.Bit, data.status.wire_feed_speed_high)
            .input('status_weld_score_low', sql.Bit, data.status.weld_score_low)
            .input('status_arc_time_out_of_limits', sql.Bit, data.status.arc_time_out_of_limits)
            .input('status_short_weld', sql.Bit, data.status.short_weld)
            .input('status_arc_time_low', sql.Bit, data.status.arc_time_low)
            .input('status_arc_time_high', sql.Bit, data.status.arc_time_high)
            .input('status_alarm', sql.Bit, data.status.alarm)
            .input('status_latch_alarm', sql.Bit, data.status.latch_alarm)
            .input('status_fault', sql.Bit, data.status.fault)
            .input('status_latch_fault', sql.Bit, data.status.latch_fault)
            .input('limits_enabled_arc_time', sql.Bit, data.limits_enabled.arc_time)
            .input('limits_enabled_weld_score', sql.Bit, data.limits_enabled.weld_score)
            .input('limits_enabled_wire_feed_speed', sql.Bit, data.limits_enabled.wire_feed_speed)
            .input('limits_enabled_current', sql.Bit, data.limits_enabled.current)
            .input('limits_enabled_voltage', sql.Bit, data.limits_enabled.voltage)
            .input('part_serial', sql.NVarChar, data.part_serial)
            .input('operator_id', sql.Int, data.operator_id)
            .input('consumable_lot', sql.NVarChar, data.consumable_lot)
            .input('weld_mode', sql.Int, data.weld_mode)
            .input('assembly_id', sql.Int, data.assembly_id)
            .input('seam_id', sql.Int, data.seam_id)
            .input('average_motor_current', sql.Float, data.average_motor_current)
            .input('average_gas_flow', sql.Float, data.average_gas_flow)
            .input('warnings_wire_feed_speed_high', sql.Bit, data.warnings.wire_feed_speed_high)
            .input('warnings_wire_feed_speed_low', sql.Bit, data.warnings.wire_feed_speed_low)
            .input('warnings_voltage_high', sql.Bit, data.warnings.voltage_high)
            .input('warnings_voltage_low', sql.Bit, data.warnings.voltage_low)
            .input('warnings_current_high', sql.Bit, data.warnings.current_high)
            .input('warnings_current_low', sql.Bit, data.warnings.current_low)
            .input('warnings_weld_score', sql.Bit, data.warnings.weld_score)
            .input('warnings_time', sql.Bit, data.warnings.time)
            .query(query);



        console.log("Data successfully inserted into database.");

    } catch (error) {
        console.error("Error processing message:", error);
    }
});
    }
});
