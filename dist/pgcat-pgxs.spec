Name:       pgcat-pgxs
Version:    0.1
Release:    1%{?dist}
Summary:    enhanced postgresql logical replication
License:    FIXME
Source0:    %{name}-%{version}.tar.gz
BuildRequires: postgresql11-devel >= 11.3
Requires:   postgresql11-server >= 11.3

%description
enhanced postgresql logical replication

%prep
%setup -q

%build
make with_llvm=no

%install
make DESTDIR=%{buildroot} with_llvm=no install

%files
/usr/pgsql-11/lib/pgcat.so
/usr/pgsql-11/share/extension/pgcat--1.0.sql
/usr/pgsql-11/share/extension/pgcat.control

%changelog
# let's skip this for now
