SELECT
	{group_by_select_clause},
	{main_total_diff_clause},
	{main_aggregate_diff_clause}
FROM
(
	SELECT {group_by_select_clause},
			{group_by_total_clause},
			{group_by_aggregate_columns_clause}
	 FROM 
	 (
		SELECT {group_by_select_clause},
			{table1_total_clause},
			{table1_aggregate_columns1_clause},
			{table1_aggregate_columns2_clause}
		from regulatory_report table1
		WHERE {snapshot_column} = :snapshot_1
		union all
		SELECT {group_by_select_clause},
			{table2_total_clause},
			{table2_aggregate_columns1_clause},
			{table2_aggregate_columns2_clause}
		from regulatory_report table2
		WHERE {snapshot_column} = :snapshot_2
	 )
	 GROUP BY  {group_by_select_clause}
) main