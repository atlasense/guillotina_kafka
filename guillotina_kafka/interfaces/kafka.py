from zope.interface import Attribute
from zope.interface import Interface


class IKafak(Interface):
    application_name = Attribute('Name of the apllication')
    host = Attribute('Kafka brocker host')
    port = Attribute('Kafka brocker port')
