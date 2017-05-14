WITH orgMembers AS
(
  SELECT our.guid AS roleOrgGuid, our.userGuid AS roleUserGuid, our.orgUserRoleType AS roleType, our.dtUpdated AS dtRoleUpdated,
  oe.displayName AS orgDisplayName, oe.dtUpdated AS dtOrgUpdated
  FROM OrgEntity AS oe JOIN OrgUserRole AS our ON (our.orgGuid = oe.guid)
),
userWithOrg AS (
  SELECT ue.guid, struct(ue.*) AS user, struct(om.*) AS orgMembership
  FROM UserEntity AS ue LEFT OUTER JOIN orgMembers AS om ON (om.roleUserGuid = ue.guid)
),
modifiedUsers AS (
  SELECT guid, first(user) as user, collect_list(orgMembership) AS orgMemberships
    FROM userWithOrg AS ue
   WHERE user.dtUpdated between "{lastRun}" AND "{thisRun}" OR
         orgMembership.dtRoleUpdated between  "{lastRun}" AND "{thisRun}" OR
         orgMembership.dtOrgUpdated between  "{lastRun}" AND "{thisRun}"
   GROUP BY guid
),
usersWithEmotions AS (
  SELECT mu.*, em.emotion FROM modifiedUsers AS mu LEFT OUTER JOIN UserEmotions AS em ON (mu.guid = em.guid)
)
SELECT 'things' as type,
  user.guid, user.identity, user.displayName, user.contactEmail, user.avatarUrl, user.gravatarEmail, user.blurb,
  user.location, user.defaultTraitPrivacyType, user.companyName, user.isActive, user.isHeadless,
  emotion, user.dtCreated, user.dtUpdated, orgMemberships FROM usersWithEmotions