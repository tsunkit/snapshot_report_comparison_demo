SELECT TABLE_NO,{snapshot_column},{UNIQUE_REFERENCE_ID},{check_details_domain_columns},{check_details_value_columns_diff}  
FROM
(
	SELECT TABLE_NO,{snapshot_column},{UNIQUE_REFERENCE_ID},{check_details_domain_columns},{check_details_value_columns}
		,{check_details_value_columns_lead_lag}                
	from 
	(
		SELECT 
			1 AS TABLE_NO,{snapshot_column},{UNIQUE_REFERENCE_ID},{check_details_domain_columns},{check_details_value_columns}
		FROM REGULATORY_REPORT
		WHERE {snapshot_column} = ?
		AND ( {check_details_where_clause} )
		UNION ALL
		SELECT 
			2 AS TABLE_NO,{snapshot_column},{UNIQUE_REFERENCE_ID},{check_details_domain_columns},{check_details_value_columns}
		FROM REGULATORY_REPORT
		WHERE {snapshot_column} = ?
		AND ( {check_details_where_clause} )
	)
	ORDER BY {UNIQUE_REFERENCE_ID}, TABLE_NO
) ORDER BY {check_details_domain_columns}, {UNIQUE_REFERENCE_ID}