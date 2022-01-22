# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'beneat_chauve_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Beneat_Chauve_PySpider_france"

    def start_requests(self):
        start_urls = [ 
            {
                "url": [
                    "https://www.beneat-chauvel.com/location/appartement",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.beneat-chauvel.com/location/maison"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
 
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'teaser__image__link')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        external_id=response.xpath("//span[contains(.,'Réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1].strip())
        rent=response.xpath("//strong[contains(.,'Loyer')]/following-sibling::text()").get()
        if rent:
            rent=rent.split("€")[0].strip().replace(" ","")
            if rent:
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//strong[contains(.,'Surface')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        utilities=response.xpath("//strong[.='Provision sur charges : ']/following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0])
        room=response.xpath("//strong[contains(.,'Nombre de prièces')]/following-sibling::text()").get()
        if room:
            item_loader.add_value("room_count",room)
        bath=response.xpath("//strong[contains(.,'Nombre de salles d')]/following-sibling::text()").get()
        if bath:
            item_loader.add_value("bathroom_count",bath)
        deposit=response.xpath("//strong[contains(.,'Dépôt de garantie')]/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.replace("€","").replace(" ",""))
        available_date=response.xpath("//strong[contains(.,'Disponibilité')]/following-sibling::text()").get()
        if available_date and not "Disponible" in available_date:
            item_loader.add_value("available_date",available_date)
        elevator=response.xpath("//strong[contains(.,'Ascenseur')]/following-sibling::text()").get()
        if elevator:
            if "Oui"==elevator:
                item_loader.add_value("elevator",True)
            if "Non"==elevator:
                item_loader.add_value("elevator",False)
        terrace=response.xpath("//strong[contains(.,'Terrasse')]/following-sibling::text()").get()
        if terrace:
            if "Oui"==terrace:
                item_loader.add_value("terrace",True)
            if "Non"==terrace:
                item_loader.add_value("terrace",False)
        floor=response.xpath("//strong[contains(.,'Étage')]/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        desc=response.xpath("//h2[.='Présentation du bien']/following-sibling::div/div/div/p/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        adres=response.xpath("//div[@class='row']//h1/following-sibling::div/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//div[@class='row']//h1/following-sibling::div/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[0])
        zipcode=response.xpath("//div[@class='row']//h1/following-sibling::div/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[-1].replace("(","").replace(")",""))
        images=[x for x in response.xpath("//meta[@property='og:image']//@content").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Agence Bénéat-Chauvel")

        phone=response.xpath("//div[contains(@class,'term-agence__phone')]/span/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        latitude=response.xpath("//script[contains(.,'lng')]//text()").get()
        if latitude:
            latitude=latitude.split('"map":{"selector')[-1].split("lat")[-1].split(",")[0].replace(":","").replace('"',"")
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'lng')]//text()").get()
        if longitude:
            longitude=longitude.split('"map":{"selector')[-1].split("lng")[-1].split(",")[0].replace(":","").replace('"',"")
            item_loader.add_value("longitude",longitude)

        yield item_loader.load_item()