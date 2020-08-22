// Let's add a fetchData method to our Table component that will be used to fetch
// new data when pagination state changes
// We can also add a loading state to let our table know it's loading new data
import {usePagination, useTable, useGlobalFilter} from "react-table";
import React, {useEffect} from "react";
import Table from "@material-ui/core/Table";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import TableBody from "@material-ui/core/TableBody";
import styled from "styled-components";

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

export const FilingGrid = ({filter, columns, data, fetchData, loading, pageCount: controlledPageCount}) => {
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

    useEffect(() => {
        setGlobalFilter(filter)
    }, [filter])

    // Listen for changes in pagination and use the state to fetch our new data
    useEffect(() => {
        fetchData({ pageIndex, pageSize })
    }, [pageIndex, pageSize, globalFilter])

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
        <Styles>
            <Table {...getTableProps()}>
                <TableHead>
                    {headerGroups.map(headerGroup => (
                        <TableRow {...headerGroup.getHeaderGroupProps()}>
                            {headerGroup.headers.map(column => (
                                <th {...column.getHeaderProps()}>
                                    {column.render('Header')}
                                    <span>{column.isSorted ? column.isSortedDesc ? ' 🔽' : ' 🔼' : ''}</span>
                                </th>
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
                                    return <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
                                })}
                            </TableRow>
                        )
                    })}
                    <TableRow>
                        {loading ? (
                            // Use our custom loading state to show a loading indicator
                            <td colSpan="10000">Loading...</td>) : (
                            <td colSpan="10000">
                                {pageIndex * page.length} to {(pageIndex + 1) * page.length} filings
                            </td>
                        )}
                    </TableRow>
                </TableBody>
            </Table>{
            // Pagination can be built however you'd like. This is just a very basic UI implementation:
        }
        <Pagination {...paginationProps}/>
        </Styles>
    )
};

const Styles = styled.div`
  padding: 1rem;

  table {
    border-spacing: 0;
    border: 1px solid black;
    width: 100%;

    tr {
      :last-child {
        td {
          border-bottom: 0;
        }
      }
    }

    th,
    td {
      margin: 0;
      padding: 0.5rem;
      border-bottom: 1px solid black;
      border-right: 1px solid black;
      text-align: center;

      :last-child {
        border-right: 0;
      }
    }
  }
  .pagination {
    padding: 0.5rem;
  }
`;
