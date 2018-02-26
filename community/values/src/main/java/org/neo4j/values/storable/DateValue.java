/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import java.lang.invoke.MethodHandle;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
import static org.neo4j.values.storable.IntegralValue.safeCastIntegral;

public final class DateValue extends TemporalValue<LocalDate,DateValue>
{
    public static DateValue date( LocalDate value )
    {
        return new DateValue( requireNonNull( value, "LocalDate" ) );
    }

    public static DateValue date( int year, int month, int day )
    {
        return new DateValue( LocalDate.of( year, month, day ) );
    }

    public static DateValue weekDate( int year, int week, int dayOfWeek )
    {
        return new DateValue( localWeekDate( year, week, dayOfWeek ) );
    }

    public static DateValue quarterDate( int year, int quarter, int dayOfQuarter )
    {
        return new DateValue( localQuarterDate( year, quarter, dayOfQuarter ) );
    }

    public static DateValue ordinalDate( int year, int dayOfYear )
    {
        return new DateValue( LocalDate.ofYearDay( year, dayOfYear ) );
    }

    public static DateValue epochDate( long epochDay )
    {
        return new DateValue( LocalDate.ofEpochDay( epochDay ) );
    }

    public static DateValue parse( CharSequence text )
    {
        return parse( DateValue.class, PATTERN, DateValue::parse, text );
    }

    public static DateValue parse( TextValue text )
    {
        return parse( DateValue.class, PATTERN, DateValue::parse, text );
    }

    public static DateValue now( Clock clock )
    {
        return new DateValue( LocalDate.now( clock ) );
    }

    public static DateValue now( Clock clock, String timezone )
    {
        return now( clock.withZone( parseZoneName( timezone ) ) );
    }

    public static DateValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return StructureBuilder.build( builder( defaultZone ), map );
    }

    public static DateValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    static StructureBuilder<AnyValue,DateValue> builder( Supplier<ZoneId> defaultZone )
    {
        return new DateBuilder<AnyValue,DateValue>()
        {
            @Override
            protected ZoneId timezone( AnyValue timezone )
            {
                return timezone == null ? defaultZone.get() : timezoneOf( timezone );
            }

            @Override
            protected DateValue selectDate( AnyValue temporal )
            {
                // TODO other cases, e.g. DateTimeValue
                if ( temporal instanceof DateValue )
                {
                    return (DateValue) temporal;
                }
                throw new IllegalArgumentException( String.format( "Cannot construct date from: %s", temporal ) );
            }

            @Override
            protected DateValue constructYear( AnyValue year )
            {
                int yearI = (int) safeCastIntegral( "year", year, 0 );
                return date( yearI, 1, 1 );
            }

            @Override
            protected DateValue constructCalendarDate( AnyValue year, AnyValue month, AnyValue day )
            {
                assertDefinedInOrder( year, "year", month, "month", day, "day" );
                int yearI = (int) safeCastIntegral( "year", year, 0 );
                int monthI = (int) safeCastIntegral( "month", month, 1 );
                int dayI = (int) safeCastIntegral( "day", day, 1 );
                return date( yearI, monthI, dayI );
            }

            @Override
            protected DateValue constructWeekDate( AnyValue year, AnyValue week, AnyValue dayOfWeek )
            {
                assertDefinedInOrder( year, "year", week, "week", dayOfWeek, "dayOfWeek" );
                int yearI = (int) safeCastIntegral( "year", year, 0 );
                int weekI = (int) safeCastIntegral( "week", week, 1 );
                int dayOfWeekI = (int) safeCastIntegral( "dayOfWeek", dayOfWeek, 1 );
                return new DateValue(
                        LocalDate.now()
                                .withYear( yearI )
                                .with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, weekI )
                                .with( ChronoField.DAY_OF_WEEK, dayOfWeekI ) );
            }

            @Override
            protected DateValue constructQuarterDate( AnyValue year, AnyValue quarter, AnyValue dayOfQuarter )
            {
                assertDefinedInOrder( year, "year", quarter, "quarter", dayOfQuarter, "dayOfQuarter" );
                int yearI = (int) safeCastIntegral( "year", year, 0 );
                int quarterI = (int) safeCastIntegral( "quarter", quarter, 1 );
                int dayOfQuarterI = (int) safeCastIntegral( "dayOfQuarter", dayOfQuarter, 1 );
                return new DateValue(
                    LocalDate.now()
                            .withYear( yearI )
                            .with( IsoFields.QUARTER_OF_YEAR, quarterI )
                            .with( IsoFields.DAY_OF_QUARTER, dayOfQuarterI ) );
            }

            @Override
            protected DateValue constructOrdinalDate( AnyValue year, AnyValue ordinalDay )
            {
                if ( year instanceof LongValue && ordinalDay instanceof LongValue )
                {
                    int yearI = (int) ((LongValue) year).value();
                    int ordinalDayI = (int) ((LongValue) ordinalDay).value();
                    return new DateValue( LocalDate.ofYearDay( yearI, ordinalDayI ) );
                }
                throw new IllegalArgumentException( String.format( "Cannot construct date from: %s %s", year, ordinalDay ) );
            }
        };
    }

    public abstract static class Compiler<Input> extends DateBuilder<Input,MethodHandle>
    {
    }

    private final LocalDate value;

    private DateValue( LocalDate value )
    {
        this.value = value;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        DateValue other = (DateValue) otherValue;
        return value.compareTo( other.value );
    }

    @Override
    LocalDate temporal()
    {
        return value;
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof DateValue && value.equals( ((DateValue) other).value );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeDate( value.toEpochDay() );
    }

    @Override
    public String prettyPrint()
    {
        return value.format( DateTimeFormatter.ISO_DATE );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.DATE;
    }

    @Override
    protected int computeHash()
    {
        return Long.hashCode( value.toEpochDay() );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapDate( this );
    }

    @Override
    public DateValue add( DurationValue duration )
    {
        return replacement( value.plusMonths( duration.totalMonths() ).plusDays( duration.totalDays() ) );
    }

    @Override
    public DateValue sub( DurationValue duration )
    {
        return replacement( value.minusMonths( duration.totalMonths() ).minusDays( duration.totalDays() ) );
    }

    @Override
    DateValue replacement( LocalDate date )
    {
        return date == value ? this : new DateValue( date );
    }

    static final boolean QUARTER_DATES = flag( DateValue.class, "QUARTER_DATES", true );
    /**
     * The regular expression pattern for parsing dates. All fields come in two versions - long and short form, the
     * long form is for formats containing dashes, the short form is for formats without dashes. The long format is
     * the only one that handles signed years, since that is how the only case that supports years with other than 4
     * numbers, and not having dashes then would make the format ambiguous. In order to not have two cases that can
     * parse only the year, we let the long case handle that.
     * <p/>
     * Valid formats:
     * <ul>
     * <li>Year:<ul>
     * <li>{@code [0-9]{4}} - unique without dashes, since it is the only one 4 numbers long<br>
     * Parsing: {@code longYear}</li>
     * <li>{@code [+-] [0-9]{1,9}}<br>
     * Parsing: {@code longYear}</li>
     * </ul></li>
     * <li>Year & Month:<ul>
     * <li>{@code [0-9]{4} [0-9]{2}} - unique without dashes, since it is the only one 6 numbers long<br>
     * Parsing: {@code shortYear, shortMonth}</li>
     * <li>{@code [0-9]{4} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth}</li>
     * <li>{@code [+-] [0-9]{1,9} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth}</li>
     * </ul></li>
     * <li>Calendar date (Year & Month & Day):<ul>
     * <li>{@code [0-9]{4} [0-9]{2} [0-9]{2}} - unique without dashes, since it is the only one 8 numbers long<br>
     * Parsing: {@code shortYear, shortMonth, shortDay}</li>
     * <li>{@code [0-9]{4} - [0-9]{1,2} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth, longDay}</li>
     * <li>{@code [+-] [0-9]{1,9} - [0-9]{1,2} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth, longDay}</li>
     * </ul></li>
     * <li>Year & Week:<ul>
     * <li>{@code [0-9]{4} W [0-9]{2}}<br>
     * Parsing: {@code shortYear, shortWeek}</li>
     * <li>{@code [0-9]{4} - W [0-9]{2}}<br>
     * Parsing: {@code longYear, longWeek}</li>
     * <li>{@code [+-] [0-9]{1,9} - W [0-9]{2}}<br>
     * Parsing: {@code longYear, longWeek}</li>
     * </ul></li>
     * <li>Week date (year & week & day of week):<ul>
     * <li>{@code [0-9]{4} W [0-9]{2} [0-9]} - unique without dashes, contains W followed by 2 numbers<br>
     * Parsing: {@code shortYear, shortWeek, shortDOW}</li>
     * <li>{@code [0-9]{4} - W [0-9]{1,2} - [0-9]}<br>
     * Parsing: {@code longYear, longWeek, longDOW}</li>
     * <li>{@code [+-] [0-9]{1,9} - W [0-9]{2} - [0-9]}<br>
     * Parsing: {@code longYear, longWeek, longDOW}</li>
     * </ul></li>
     * <li>Ordinal date (year & day of year):<ul>
     * <li>{@code [0-9]{4} [0-9]{3}} - unique without dashes, since it is the only one 7 number long<br>
     * Parsing: {@code shortYear, shortDOY}</li>
     * <li>{@code [0-9]{4} - [0-9]{3}} - needs to be exactly 3 numbers long to distinguish from Year & Month<br>
     * Parsing: {@code longYear, longDOY}</li>
     * <li>{@code [+-] [0-9]{1,9} - [0-9]{3}} - needs to be exactly 3 numbers long to distinguish from Year & Month<br>
     * Parsing: {@code longYear, longDOY}</li>
     * </ul></li>
     * </ul>
     */
    static final String DATE_PATTERN = "(?:"
            // short formats - without dashes:
            + "(?<shortYear>[0-9]{4})(?:"
            + "(?<shortMonth>[0-9]{2})(?<shortDay>[0-9]{2})?|" // calendar date
            + "W(?<shortWeek>[0-9]{2})(?<shortDOW>[0-9])?|" // week date
            + (QUARTER_DATES ? "Q(?<shortQuarter>[0-9])(?<shortDOQ>[0-9]{2})?|" : "") // quarter date
            + "(?<shortDOY>[0-9]{3}))" + "|" // ordinal date
            // long formats - includes dashes:
            + "(?<longYear>(?:[0-9]{4}|[+-][0-9]{1,9}))(?:"
            + "-(?<longMonth>[0-9]{1,2})(?:-(?<longDay>[0-9]{1,2}))?|" // calendar date
            + "-W(?<longWeek>[0-9]{1,2})(?:-(?<longDOW>[0-9]))?|" // week date
            + (QUARTER_DATES ? "-Q(?<longQuarter>[0-9])(?:-(?<longDOQ>[0-9]{1,2}))?|" : "") // quarter date
            + "-(?<longDOY>[0-9]{3}))?" + ")"; // ordinal date
    private static final Pattern PATTERN = Pattern.compile( DATE_PATTERN );

    /**
     * Creates a {@link LocalDate} from a {@link Matcher} that matches the regular expression defined by
     * {@link #DATE_PATTERN}. The decision tree in the implementation of this method is guided by the parsing notes
     * for {@link #DATE_PATTERN}.
     *
     * @param matcher
     *         a {@link Matcher} that matches the regular expression defined in {@link #DATE_PATTERN}.
     * @return a {@link LocalDate} parsed from the given {@link Matcher}.
     */
    static LocalDate parseDate( Matcher matcher )
    {
        String longYear = matcher.group( "longYear" );
        if ( longYear != null )
        {
            return parse( matcher, parseInt( longYear ),
                    "longMonth", "longDay", "longWeek", "longDOW", "longQuarter", "longDOQ", "longDOY" );
        }
        else
        {
            return parse( matcher, parseInt( matcher.group( "shortYear" ) ),
                    "shortMonth", "shortDay", "shortWeek", "shortDOW", "shortQuarter", "shortDOQ", "shortDOY" );
        }
    }

    private static LocalDate parse(
            Matcher matcher, int year,
            String MONTH, String DAY, String WEEK, String DOW, String QUARTER, String DOQ, String DOY )
    {
        String month = matcher.group( MONTH );
        if ( month != null )
        {
            return LocalDate.of( year, parseInt( month ), optInt( matcher.group( DAY ) ) );
        }
        String week = matcher.group( WEEK );
        if ( week != null )
        {
            return localWeekDate( year, parseInt( week ), optInt( matcher.group( DOW ) ) );
        }
        String quarter = matcher.group( QUARTER );
        if ( quarter != null )
        {
            return localQuarterDate( year, parseInt( quarter ), optInt( matcher.group( DOQ ) ) );
        }
        String doy = matcher.group( DOY );
        if ( doy != null )
        {
            return LocalDate.ofYearDay( year, parseInt( doy ) );
        }
        return LocalDate.of( year, 1, 1 );
    }

    private static DateValue parse( Matcher matcher )
    {
        return new DateValue( parseDate( matcher ) );
    }

    private static int optInt( String value )
    {
        return value == null ? 1 : parseInt( value );
    }

    private static LocalDate localWeekDate( int year, int week, int dayOfWeek )
    {
        LocalDate weekOne = LocalDate.of( year, 1, 4 ); // the fourth is guaranteed to be in week 1 by definition
        LocalDate withWeek = weekOne.with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, week );
        // the implementation of WEEK_OF_WEEK_BASED_YEAR uses addition to adjust the date, this means that it accepts
        // week 53 of years that don't have 53 weeks, so we have to guard for this:
        if ( week == 53 && withWeek.get( IsoFields.WEEK_BASED_YEAR ) != year )
        {
            throw new DateTimeException( String.format( "Year %d does not contain %d weeks.", year, week ) );
        }
        return withWeek.with( ChronoField.DAY_OF_WEEK, dayOfWeek );
    }

    private static LocalDate localQuarterDate( int year, int quarter, int dayOfQuarter )
    {
        // special handling for the range of Q1 and Q2, since they are shorter than Q3 and Q4
        if ( quarter == 2 && dayOfQuarter == 92 )
        {
            throw new DateTimeException( "Quarter 2 only has 91 days." );
        }
        // instantiate the yearDate now, because we use it to know if it is a leap year
        LocalDate yearDate = LocalDate.ofYearDay( year, dayOfQuarter ); // guess on the day
        if ( quarter == 1 && dayOfQuarter > 90 && (!yearDate.isLeapYear() || dayOfQuarter == 92) )
        {
            throw new DateTimeException( String.format(
                    "Quarter 1 of %d only has %d days.", year, yearDate.isLeapYear() ? 91 : 90 ) );
        }
        return yearDate
                .with( IsoFields.QUARTER_OF_YEAR, quarter )
                .with( IsoFields.DAY_OF_QUARTER, dayOfQuarter );
    }

    private abstract static class DateBuilder<Input, Result> extends Builder<Input,Result>
    {
        @Override
        protected final boolean supportsDate()
        {
            return true;
        }

        @Override
        protected final boolean supportsTime()
        {
            return false;
        }

        @Override
        protected final Result selectDateTime( Input temporal )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result selectDateAndTime( Input date, Input time )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result selectDateWithConstructedTime(
                Input date,
                Input hour,
                Input minute,
                Input second,
                Input millisecond,
                Input microsecond,
                Input nanosecond )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result selectTime( Input temporal )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructCalendarDateWithSelectedTime( Input year, Input month, Input day, Input time )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructCalendarDateWithConstructedTime(
                Input year,
                Input month,
                Input day,
                Input hour,
                Input minute,
                Input second,
                Input millisecond,
                Input microsecond,
                Input nanosecond )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructWeekDateWithSelectedTime( Input year, Input week, Input dayOfWeek, Input time )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructWeekDateWithConstructedTime(
                Input year,
                Input week,
                Input dayOfWeek,
                Input hour,
                Input minute,
                Input second,
                Input millisecond,
                Input microsecond,
                Input nanosecond )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructQuarterDateWithSelectedTime(
                Input year,
                Input quarter,
                Input dayOfQuarter,
                Input time )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructQuarterDateWithConstructedTime(
                Input year,
                Input quarter,
                Input dayOfQuarter,
                Input hour,
                Input minute,
                Input second,
                Input millisecond,
                Input microsecond,
                Input nanosecond )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructOrdinalDateWithSelectedTime( Input year, Input ordinalDay, Input time )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructOrdinalDateWithConstructedTime(
                Input year,
                Input ordinalDay,
                Input hour,
                Input minute,
                Input second,
                Input millisecond,
                Input microsecond,
                Input nanosecond )
        {
            throw new IllegalStateException( "time not supported" );
        }

        @Override
        protected final Result constructTime(
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            throw new IllegalStateException( "time not supported" );
        }
    }
}
