/**
 * Shared IAM utility functions for the SeaweedFS Admin Dashboard.
 */

// URL prefix helper for subdirectory deployment
function iamBasePath(path) {
    return (window.__BASE_PATH__ || '') + path;
}

// Delete user function
async function deleteUser(username) {
    showDeleteConfirm(username, async function () {
        try {
            const encodedUsername = encodeURIComponent(username);
            const response = await fetch(iamBasePath(`/api/users/${encodedUsername}`), {
                method: 'DELETE'
            });

            if (response.ok) {
                showAlert('用户删除成功', 'success');
                setTimeout(() => window.location.reload(), 1000);
            } else {
                const error = await response.json().catch(() => ({}));
                showAlert('删除用户失败:' + (error.error || '未知错误'), 'error');
            }
        } catch (error) {
            console.error('Error deleting user:', error);
            showAlert('删除用户失败:' + error.message, 'error');
        }
    }, '确定要删除此用户吗?此操作不可撤销。');
}

// Delete group function
async function deleteGroup(name) {
    showDeleteConfirm(name, async function () {
        try {
            const encodedName = encodeURIComponent(name);
            const response = await fetch(iamBasePath(`/api/groups/${encodedName}`), {
                method: 'DELETE'
            });

            if (response.ok) {
                showAlert('组删除成功', 'success');
                setTimeout(() => window.location.reload(), 1000);
            } else {
                const error = await response.json().catch(() => ({}));
                showAlert('删除组失败:' + (error.error || '未知错误'), 'error');
            }
        } catch (error) {
            console.error('Error deleting group:', error);
            showAlert('删除组失败:' + error.message, 'error');
        }
    }, '确定要删除此组吗?此操作不可撤销。');
}

// Delete access key function
async function deleteAccessKey(username, accessKey) {
    showDeleteConfirm(accessKey, async function () {
        try {
            const encodedUsername = encodeURIComponent(username);
            const encodedAccessKey = encodeURIComponent(accessKey);
            const response = await fetch(iamBasePath(`/api/users/${encodedUsername}/access-keys/${encodedAccessKey}`), {
                method: 'DELETE'
            });

            if (response.ok) {
                showAlert('访问密钥删除成功', 'success');
                // If refreshAccessKeysList exists (in object_store_users.templ), use it
                if (typeof refreshAccessKeysList === 'function') {
                    refreshAccessKeysList(username);
                } else {
                    setTimeout(() => window.location.reload(), 1000);
                }
            } else {
                const error = await response.json().catch(() => ({}));
                showAlert('删除访问密钥失败:' + (error.error || '未知错误'), 'error');
            }
        } catch (error) {
            console.error('Error deleting access key:', error);
            showAlert('删除访问密钥失败:' + error.message, 'error');
        }
    }, '确定要删除此访问密钥吗?');
}
