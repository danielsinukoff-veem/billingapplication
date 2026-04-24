window.BILLING_APP_CONFIG = {
  ...(window.BILLING_APP_CONFIG || {}),
  authMethod: "aws-cognito",
  awsRegion: "us-east-1",
  workspaceLabel: "Veem Billing Workspace",
  bootstrapUrl: "https://YOUR-BUCKET.s3.us-east-1.amazonaws.com/current/workbook.json",
  workbookReadUrl: "https://YOUR-BUCKET.s3.us-east-1.amazonaws.com/current/workbook.json",
  workbookWriteUrl: "https://YOUR-BUCKET.s3.us-east-1.amazonaws.com/current/workbook.json",
  workbookHistoryWriteBaseUrl: "https://YOUR-BUCKET.s3.us-east-1.amazonaws.com/history/workbook/",
  enableSharedWorkbook: true,
};
