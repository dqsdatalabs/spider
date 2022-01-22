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
    name = 'dechatouville-immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="DechatouvilleImmobilier_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.dechatouville-immobilier.com/a-louer/appartements/1",
                ],
                "property_type":"apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='card col']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type" : response.meta.get("property_type")},)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type="".join(response.xpath("//ol[@class='breadcrumb']//li//text()").getall())
        if property_type:
            if "Maison" in property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
        adres=response.xpath("//th[.='Ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres.strip())
        city=response.xpath("//th[.='Ville']/text()").get()
        if city:
            item_loader.add_value("city",city.strip())
        zipcode=response.xpath("//th[.='Code postal']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        
        square_meters=response.xpath("//th[.='Surface habitable (m²)']/following-sibling::th/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].split(",")[0].strip())
        room_count=response.xpath("//th[.='Nombre de pièces']/following-sibling::th/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//th[.='Nb de salle de bains']/following-sibling::th/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        furnished=response.xpath("//th[.='Meublé']/following-sibling::th/text()").get()
        if furnished and furnished=="OUI":
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//th[.='Ascenseur']/following-sibling::th/text()").get()
        if elevator and elevator=="OUI":
            item_loader.add_value("elevator",True)
        terrace=response.xpath("//th[.='Terrasse']/following-sibling::th/text()").get()
        if terrace and terrace=="OUI":
            item_loader.add_value("terrace",True)
        floor=response.xpath("//th[.='Etage']/following-sibling::th/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        rent=response.xpath("//th[.='Loyer CC* / mois']/following-sibling::th/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//th[.='Honoraires TTC charge locataire']/following-sibling::th/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(",")[0].strip())
        deposit=response.xpath("//th[.='Dépôt de garantie TTC']/following-sibling::th/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].split(",")[0].strip())
        img=[]
        images=response.xpath("//img//@src").getall()
        if images:
            for i in images:
                if "staticlbi.com" in i:
                    img.append(i)
                    item_loader.add_value("images",img)
        description=response.xpath("//p[@class='description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        external_id="".join(response.xpath("//span[@class='labelprix ref']/following-sibling::text()").get())
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\n","").strip())
        item_loader.add_value("landlord_name","De Chatouville Immobilier")



        yield item_loader.load_item()