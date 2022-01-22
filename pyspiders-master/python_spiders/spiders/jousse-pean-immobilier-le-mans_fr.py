# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re 
import dateparser
class MySpider(Spider):
    name = 'jousse-pean-immobilier-le-mans_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="JoussePeanImmobilierLeMans_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    formdata = {
        "typeBien": "maison;Maison/villa",
        "secteurGeo": "SARTHE",
        "prix": "0",
        "dpe": "0",
        "nbChambres": "0",
        "ref": ""
    }
    
    def start_requests(self):
        start_urls = [
            {
                "type": "appartement",
                "property_type": "apartment"
            },
	        {
                "type": "maison;Maison/villa",
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            self.formdata["typeBien"] = url.get('type')
            yield FormRequest(
                url="https://www.jousse-pean-immobilier-le-mans.fr/location.html",
                callback=self.parse,
                formdata=self.formdata,
                dont_filter=True,
                meta={
                    'property_type': url.get('property_type'),
                }
            )
    # 1. FOLLOWING
    def parse(self, response):

        for item in  response.xpath("//a[@class='vignetteAnnonce']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//p[@class='ref']/following-sibling::h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        external_id=response.xpath("//p[@class='ref']/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        item_loader.add_value("address","Le Mans")
        rent=response.xpath("//p[@class='prix text-right']/strong/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].split("€")[0])
        item_loader.add_value("currency","EUR")
        deposit=response.xpath("//p[contains(.,'Dépot de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].strip())
        description=response.xpath("//p[@class='descriptif']/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//a[@class='fancybox']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//td[.='Surface habitable']/following-sibling::td[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].split("m²")[0])
        room_count=response.xpath("//td[.='Nombre de pièces']/following-sibling::td[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//td[contains(.,'Salles de bain/d')]/following-sibling::td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        terrace=response.xpath("//td[.='Balcon/Terrasse']/following-sibling::td[2]/text()").get()
        if terrace and "non" in terrace.lower():
            item_loader.add_value("terrace",False)
        if terrace and "oui" in terrace.lower():
            item_loader.add_value("terrace",True)
        parking=response.xpath("//td[.='Garage/Parking']/following-sibling::td[2]/text()").get()
        if parking and "non" in parking.lower():
            item_loader.add_value("parking",False)
        if parking and "oui" in parking.lower():
            item_loader.add_value("parking",True)
        energy_label=response.xpath("//img[contains(@src,'dpe')]/@src").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("dpe/")[-1].split("/")[0])
        item_loader.add_value("landlord_name","Cabinet Immobilier JOUSSE PEAN")
        item_loader.add_value("landlord_phone","02 43 77 15 15")
        item_loader.add_value("landlord_email","cabimmo.jousse-pean@wanadoo.fr")

        yield item_loader.load_item()