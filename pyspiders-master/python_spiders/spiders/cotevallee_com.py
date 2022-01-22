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

class MySpider(Spider):
    name = 'cotevallee_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://cotevallee.com/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Appartement&surface_habitable_min=&chambre_min=&prix_max=&orderby=bien.dcre+desc",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://cotevallee.com/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Maison&surface_habitable_min=&chambre_min=&prix_max=&orderby=bien.dcre+desc",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img-wr']"):
            status = item.xpath("./span/text()").get()
            if status and "loué" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(.,'Suivant')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cotevallee_PySpider_france")
        item_loader.add_value("external_link", response.url)

        title = "".join(response.xpath("//h2//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//p[contains(.,'Locali')]/text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[2].strip())
            item_loader.add_value("city", address.split(":")[2].strip())
            zipcode = address.split(":")[1].split("-")[0].strip()
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//p[contains(.,'Prix')]/text()").get()
        if rent:
            rent = rent.split(":")[1].split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

        furnished = response.xpath("//div[@id='desc']//text()[contains(.,'meublée')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        external_id = response.xpath("//p[contains(.,'Réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        bathroom_count = response.xpath("//li[contains(.,'Salles')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].strip().split(" ")[0])
        
        desc = "".join(response.xpath("//div[@id='desc']/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        room_count = response.xpath("//li[contains(.,'Chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        
        if "de garantie" in desc:
            deposit = desc.split("de garantie")[1].split("\u20ac")[0].strip().replace("\u00a0","")
            item_loader.add_value("deposit", deposit)
        
        if "des lieux" in desc:
            utilities = desc.split("des lieux")[1].split("\u20ac")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        images = response.xpath("//script[contains(.,'image')]/text()").get()
        images = images.split("href : '")
        for i in range(1,len(images)):
             item_loader.add_value("images", images[i].split("'")[0])
        
        item_loader.add_value("landlord_name", "COTE VALLEE")
        
        landlord_phone = response.xpath("//div[@class='adr-agence']//text()[contains(.,'Tél')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[1].strip())


        script_data = response.xpath("//script[contains(.,'maps.LatLng')]//text()").get()
        if script_data:
            latlng = script_data.split("maps.LatLng(")[1].split(");")[0].strip()
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        
        
        yield item_loader.load_item()