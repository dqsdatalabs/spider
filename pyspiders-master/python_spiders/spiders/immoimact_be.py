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
    name = 'immoimact_be'
    execution_type='testing'
    country='belgium'
    locale='en'
    external_source = "Immo_Imact_PySpider_belgium"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immo-imact.be/recherche.php?vendreLouer=1&searchTypeBien=Appartement&varPrice=&varPriceL=&SearchLieu=", "property_type": "apartment"},
	        {"url": "https://www.immo-imact.be/recherche.php?vendreLouer=1&searchTypeBien=Bungalow&varPrice=&varPriceL=&SearchLieu=", "property_type": "house"},
            {"url": "https://www.immo-imact.be/recherche.php?vendreLouer=1&searchTypeBien=Flat%2Fstudio&varPrice=&varPriceL=&SearchLieu=", "property_type": "studio"},
            {"url": "https://www.immo-imact.be/recherche.php?vendreLouer=1&searchTypeBien=Maison&varPrice=&varPriceL=&SearchLieu=", "property_type": "house"},    
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@class='slideNews']/div[@class='item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_source", self.external_source)
        
        rent = "".join(response.xpath("//span[@class='price']/text()").extract())
        if rent:
            price =  rent.replace(",",".").replace(" ","").split("€")[0].strip()
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")

        meters = "".join(response.xpath("//li[span[.='Surface habitable']]/em/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        room = "".join(response.xpath("//div[@class='details']/ul/li[@class='chambre']/text()").extract())
        if room:
            item_loader.add_value("room_count", room.split("chambre")[0].strip())

        bathroom_count = "".join(response.xpath("//div[@class='details']/ul/li[@class='douche']/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("douche")[0].strip())
        else:
            bathroom_count = "".join(response.xpath("//div[@class='details']/ul/li[@class='sdb']/text()").extract())
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].strip())

        energy_label = "".join(response.xpath("substring-before(substring-after(//h2[@class='sousTitre']/img/@src,'_'),'.')").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())

        desc = "".join(response.xpath("//div[@class='wrapper']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        address = "".join(response.xpath("substring-after(//title/text(),'- ')").extract())
        if address:
            zipcode = address.split(" ")[0]
            city = address.split(" ")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())

        images = [x for x in response.xpath("//div[@class='slidePhoto']//div/a/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        phone = " ".join(response.xpath("//div[@class='contactInfos']//a/@href[contains(.,'tel:')]").getall()).strip()   
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[1].strip())

        email = " ".join(response.xpath("//div[@class='contactInfos']//a/@href[contains(.,'mailto:')]").getall()).strip()   
        if email:
            item_loader.add_value("landlord_email", email.split(":")[1].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='infoSupp']/span/text()")

        yield item_loader.load_item()