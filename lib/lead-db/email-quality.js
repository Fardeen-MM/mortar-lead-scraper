/**
 * Lead Database — Email classification, validation, compliance
 */
const all = require('./_all');

module.exports = {
  classifyEmail: all.classifyEmail,
  classifyAllEmails: all.classifyAllEmails,
  getEmailClassification: all.getEmailClassification,
  validateEmailSyntax: all.validateEmailSyntax,
  validateEmailMX: all.validateEmailMX,
  batchValidateEmails: all.batchValidateEmails,
  exportForInstantly: all.exportForInstantly,
  getVerificationStats: all.getVerificationStats,
  bulkImportVerification: all.bulkImportVerification,
  recordConsent: all.recordConsent,
  addOptOut: all.addOptOut,
  removeOptOut: all.removeOptOut,
  getComplianceDashboard: all.getComplianceDashboard,
  checkEmailCompliance: all.checkEmailCompliance,
  getEmailDeliverability: all.getEmailDeliverability,
};
