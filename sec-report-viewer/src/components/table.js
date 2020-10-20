// Let's add a fetchData method to our Table component that will be used to fetch
// new data when pagination state changes
// We can also add a loading state to let our table know it's loading new data
import {usePagination, useTable, useGlobalFilter, Table} from "react-table";
import React, {useEffect, useState} from "react";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import TableBody from "@material-ui/core/TableBody";
import styled from "styled-components";
import TableCell from "@material-ui/core/TableCell";
import TableContainer from "@material-ui/core/TableContainer";
import Paper from "@material-ui/core/Paper";
import {makeStyles} from "@material-ui/core/styles";

const Pagination = ({gotoPage, previousPage, nextPage, canPreviousPage, canNextPage, pageCount, pageIndex, setPageSize, pageSize}) => {
    return (<div className="pagination">
        <button onClick={() => gotoPage(0)} disabled={!canPreviousPage}>
            {'<<'}
        </button>
        {' '}
        <button onClick={() => previousPage()} disabled={!canPreviousPage}>
            {'<'}
        </button>
        {' '}
        <button onClick={() => nextPage()} disabled={!canNextPage}>
            {'>'}
        </button>
        {' '}
        <button onClick={() => gotoPage(pageCount - 1)} disabled={!canNextPage}>
            {'>>'}
        </button>
        {' '}
        <span>Page{' '}<strong>{pageIndex + 1}</strong>{' '}</span>
        <select
            value={pageSize}
            onChange={e => {
                setPageSize(Number(e.target.value))
            }}>
            {[20, 50, 100].map(pageSize => (
                <option key={pageSize} value={pageSize}>
                    Show {pageSize}
                </option>
            ))}
        </select>
    </div>)
}

export const cellWithTwoValues = (right, left) => {
    return (<>
        <span style={{float: "left"}}>{right}</span>
        <span style={{float: "right"}}>{left}</span>
    </>)
}

export const metric = (right, left) => {
    const color = left > 0 ? "green" : "red"
    const increaseStr = left > 0 ? `+${left}` : left
    return (<>
        <span style={{float: "left"}}>{right}</span>
        <span style={{float: "right", color: color}}>({increaseStr}%)</span>
    </>)
}

export const formatNumber = (num) => {
    let number = Number.parseFloat(num);
    if (!Number.isNaN(number)) {
        return number.toFixed(2)
    } else {
        return "-"
    }
}

export const formatMetric = (value, yearToYearRatio) => {
    const numValue = Number.parseFloat(value)
    if (Number.isNaN(numValue)) {
        return (<span>-</span>)
    } else {
        const yearToYearInPercents = formatNumber((Number.parseFloat(yearToYearRatio) - 1.0) * 100.0)
        return metric(numValue.toFixed(2), `${yearToYearInPercents}`)
    }
}

const filterNullQueryParams = params => {
    return Object.keys(params)
        .filter(param => params[param])
        .reduce((res, param) => {
            res[param] = params[param]
        }, {})
}

const getTargetUrl = (endPoint, filter, pageSize, pageIndex) => {
    let url = new URL(endPoint)

    url.searchParams.append("limit", pageSize)
    url.searchParams.append("offset", `${pageIndex * pageSize}`)

    Object.keys(filter).forEach(key => {
        if ((filter[key] && filter[key] !== "") || filter[key] === 0) url.searchParams.append(key, filter[key])
    })

    return url;
}

export const HeaderRow = ({ columns }) => {
    // Use the state and functions returned from useTable to build your UI
    const {
        getTableProps,
        headerGroups,
    } = useTable({
        columns,
        data: [],
    })

    return (
            <TableHead>
                {headerGroups.map(headerGroup => (
                    <TableRow {...headerGroup.getHeaderGroupProps()}>
                        {headerGroup.headers.map(column => (
                            <TableCell {...column.getHeaderProps()}>
                                {column.render('Header')}
                                <span>{column.isSorted ? column.isSortedDesc ? ' 🔽' : ' 🔼' : ''}</span>
                            </TableCell>
                        ))}
                    </TableRow>
                ))}
            </TableHead>
    )
}

export const SimpleGrid = ({ columns, data }) => {
    // Use the state and functions returned from useTable to build your UI
    const {
        getTableProps,
        getTableBodyProps,
        headerGroups,
        rows,
        prepareRow,
    } = useTable({
        columns,
        data,
    })

    // Render the UI for your table
    return (
        <StyledTable {...getTableProps()}>
            <HeaderRow columns={columns}/>
            <TableBody {...getTableBodyProps()}>
            {rows.map((row, i) => {
                prepareRow(row)
                return (
                    <TableRow {...row.getRowProps()}>
                        {row.cells.map(cell => {
                            return <TableCell {...cell.getCellProps(
                                [
                                    {
                                        className: cell.column.className,
                                        style: cell.column.style,
                                        size: 'small'
                                    },
                                ]
                            )}>{cell.render('Cell')}</TableCell>
                        })}
                    </TableRow>
                )
            })}
            </TableBody>
        </StyledTable>
    )
}

export const PagedGrid = ({endPoint, filter, columns, pageCount: controlledPageCount}) => {

    useEffect(() => {
        setGlobalFilter(filter)
    }, [filter])

    const [loading, setLoading] = useState(false);
    const [data, setData] = useState([]);

    const fetchData = async function({ pageSize, pageIndex }) {
        // Give this fetch an ID
        const fetchId = ++fetchIdRef.current

        // Set the loading state
        setLoading(true);
        if (fetchId === fetchIdRef.current) {
            let url = getTargetUrl(endPoint, filter, pageSize, pageIndex);

            let fetchedFilings = await fetch(url);

            setData(await fetchedFilings.json());
            setLoading(false)
        }
    };

    const fetchIdRef = React.useRef(0);

    const {
        getTableProps,
        getTableBodyProps,
        headerGroups,
        prepareRow,
        page,
        canPreviousPage,
        canNextPage,
        pageOptions,
        pageCount,
        gotoPage,
        nextPage,
        previousPage,
        setPageSize,
        setGlobalFilter,
        // Get the state from the instance
        state: { pageIndex, pageSize, globalFilter },
    } = useTable(
        {
            columns,
            data,
            initialState: {
                pageIndex: 0,
                pageSize: 100,
            }, // Pass our hoisted table state
            manualPagination: true, // Tell the usePagination
            // hook that we'll handle our own data fetching
            // This means we'll also have to provide our own
            // pageCount.
            // pageCount: controlledPageCount,
            pageCount: -1,
            manualGlobalFilter: true
        },
        useGlobalFilter,
        usePagination
    );

    const useStyles = makeStyles({
        table: {
            minWidth: 650,
            width: "100%",
        },
    });

    // Listen for changes in pagination and use the state to fetch our new data
    useEffect(() => {
        fetchData({ pageIndex, pageSize })
    }, [pageIndex, pageSize, globalFilter])

    const classes = useStyles();

    const paginationProps = {canPreviousPage,
        canNextPage,
        pageOptions,
        pageCount,
        gotoPage,
        nextPage,
        pageIndex,
        previousPage,
        setPageSize}
    // Render the UI for your table
    return (
        <>
            <TableContainer component={Paper}>
                <StyledTable className={classes.table}{...getTableProps()}>
                    <TableHead>
                        {headerGroups.map(headerGroup => (
                            <TableRow {...headerGroup.getHeaderGroupProps()}>
                                {headerGroup.headers.map(column => (
                                    <TableCell {...column.getHeaderProps()}>
                                        {column.render('Header')}
                                        <span>{column.isSorted ? column.isSortedDesc ? ' 🔽' : ' 🔼' : ''}</span>
                                    </TableCell>
                                ))}
                            </TableRow>
                        ))}
                    </TableHead>
                    <TableBody {...getTableBodyProps()}>
                        {page.map((row, i) => {
                            prepareRow(row);
                            return (
                                <TableRow {...row.getRowProps()}>
                                    {row.cells.map(cell => {
                                        return <TableCell{...cell.getCellProps(
                                            [
                                                {
                                                    className: cell.column.className,
                                                    style: cell.column.style,
                                                    size: 'small'
                                                },
                                            ]
                                        )}>{cell.render('Cell')}</TableCell>
                                    })}
                                </TableRow>
                            )
                        })}
                        <TableRow>
                            {loading ? (
                                // Use our custom loading state to show a loading indicator
                                <TableCell colSpan="10000">Loading...</TableCell>) : (
                                <TableCell colSpan="10000">
                                    {pageIndex * page.length} to {(pageIndex + 1) * page.length} filings
                                </TableCell>
                            )}
                        </TableRow>
                    </TableBody>
                </StyledTable>{
                // Pagination can be built however you'd like. This is just a very basic UI implementation:
            }
                <Pagination {...paginationProps}/>
            </TableContainer>
        </>
    )
};

export const StyledTable = styled.table`
`;
