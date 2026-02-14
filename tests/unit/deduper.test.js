const Deduper = require('../../lib/deduper');

const existingLeads = [
  {
    first_name: 'John', last_name: 'Smith', firm_name: 'Smith Law LLC',
    city: 'Miami', state: 'FL', phone: '(305) 555-1234',
    website: 'https://www.smithlaw.com', email: 'john@smithlaw.com',
  },
  {
    first_name: 'Jane', last_name: 'Doe', firm_name: 'Doe & Associates',
    city: 'Orlando', state: 'FL', phone: '(407) 555-5678',
    website: 'https://www.doelaw.com', email: 'jane@doelaw.com',
  },
];

describe('Deduper', () => {
  let deduper;

  beforeEach(() => {
    deduper = new Deduper(existingLeads);
  });

  test('Rule 1: matches on website domain', () => {
    const result = deduper.check({
      first_name: 'Bob', last_name: 'Jones',
      website: 'https://www.smithlaw.com/attorneys',
    });
    expect(result.isDuplicate).toBe(true);
    expect(result.matchReason).toContain('domain');
  });

  test('Rule 2: matches on phone digits', () => {
    const result = deduper.check({
      first_name: 'Bob', last_name: 'Jones',
      phone: '+1 305-555-1234',
    });
    expect(result.isDuplicate).toBe(true);
    expect(result.matchReason).toContain('phone');
  });

  test('Rule 3: matches on email', () => {
    const result = deduper.check({
      first_name: 'Bob', last_name: 'Jones',
      email: 'John@SmithLaw.com',
    });
    expect(result.isDuplicate).toBe(true);
    expect(result.matchReason).toContain('email');
  });

  test('Rule 4: matches on name + city', () => {
    const result = deduper.check({
      first_name: 'John', last_name: 'Smith',
      city: 'Miami', state: 'FL',
    });
    expect(result.isDuplicate).toBe(true);
    expect(result.matchReason).toContain('name+city');
  });

  test('Rule 5: matches on fuzzy firm + state', () => {
    const result = deduper.check({
      first_name: 'Bob', last_name: 'Jones',
      firm_name: 'Smith Law', state: 'FL',
    });
    expect(result.isDuplicate).toBe(true);
    expect(result.matchReason).toContain('firm+state');
  });

  test('unique record passes all rules', () => {
    const result = deduper.check({
      first_name: 'Alice', last_name: 'Wonder',
      firm_name: 'Wonder Legal PA', city: 'Tampa', state: 'FL',
      phone: '(813) 555-9999', website: 'https://wonderlegal.com',
      email: 'alice@wonderlegal.com',
    });
    expect(result.isDuplicate).toBe(false);
    expect(result.matchReason).toBeNull();
  });

  test('addToKnown prevents intra-run duplicates', () => {
    const record = {
      first_name: 'Alice', last_name: 'Wonder',
      email: 'alice@wonderlegal.com',
    };
    expect(deduper.check(record).isDuplicate).toBe(false);
    deduper.addToKnown(record);
    expect(deduper.check(record).isDuplicate).toBe(true);
  });

  test('stats tracking', () => {
    deduper.check({ first_name: 'Bob', phone: '305-555-1234' });
    deduper.check({ first_name: 'Alice', last_name: 'New', city: 'Tampa' });
    const stats = deduper.getStats();
    expect(stats.checked).toBe(2);
    expect(stats.duplicates).toBe(1);
    expect(stats.unique).toBe(1);
  });

  test('empty existing leads — everything is unique', () => {
    const emptyDeduper = new Deduper([]);
    const result = emptyDeduper.check({
      first_name: 'John', last_name: 'Smith', city: 'Miami',
    });
    expect(result.isDuplicate).toBe(false);
  });
});
