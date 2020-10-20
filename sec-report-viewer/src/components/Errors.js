import {cellWithTwoValues, PagedGrid, formatMetric, formatNumber} from "./table";
import React, {useState} from "react";

const configureColumns = () => [
    {
        Header: 'With no revenue',
        columns: [
            {
                Header: 'Company',
                accessor: row => ({name: row.name, ticker: row.ticker}),
                Cell: ({value}) => cellWithTwoValues(value.name, value.ticker)
            },
            {
                Header: 'Current EPS',
                accessor: row => formatNumber(row.eps),
            },
            {
                Header: 'Revenue ($ millions)',
                accessor: row => formatNumber(row.revenue),
            },
            {
                Header: 'Net Income ($ millions)',
                accessor: row => formatNumber(row.netIncome),
            }

        ],
    }
];
