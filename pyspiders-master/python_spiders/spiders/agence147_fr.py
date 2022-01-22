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
    name = 'agence147_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Agence147_PySpider_france'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence147.fr/recherche/1",
                ],
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='titleArticle']"):
            follow_url = item.xpath(".//h2/a/@href").get()
            if follow_url and "/vente/" in follow_url:
                continue
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:            
            p_url = f"https://www.agence147.fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        if response.url and "appartement" in response.url:
            prop_type = "apartment"
        elif response.url and "maison" in response.url:
            prop_type = "house"
        elif response.url and "studio" in response.url:
            prop_type = "studio"
        else:
            return
        item_loader.add_value("property_type", prop_type)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1[@class='titleBien']/text()")
        external_id = response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        zipcode = response.xpath("//li[@class='data  cp']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[-1].strip())
        city = response.xpath("//li[@class='data  VillePubliqueName']/text()").get()
        if city:
            item_loader.add_value("city", city.split(":")[-1].strip())
            item_loader.add_value("address", city.split(":")[-1].strip())
        description = " ".join(response.xpath("//div[@class='offreContent']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//li[@class='data  surfaceHabitable']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[-1].split(",")[0].split("m")[0].strip())
        rent = response.xpath("//li[@class='data  formatPrix']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//li[@class='data  formatteddepotgarantie']/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].split(",")[0].replace(" ","").strip())
        utilities = response.xpath("//li[@class='data  ChargesAnnonce']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1].split(",")[0].strip())
        lat_lng = response.xpath("//script[contains(.,'center: { lat :')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())
       
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slider_Mdl']/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)  
        item_loader.add_xpath("landlord_name", "//div[@class='media negociateur']//p[@class='nom']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='media negociateur']//p[@class='nom']/following-sibling::p[1]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='media negociateur']//p[@class='mail']/a/text()")
        bathroom_count = response.xpath("//li[@class='data  NB_SE']/text() | //li[@class='data  NB_SDB']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[-1])
        room_count = response.xpath("//li[@class='data  nbpieces']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[-1])
        else:
            item_loader.add_xpath("room_count", "substring-after(//li[@class='data  nbpieces']/text(),':')")
        
        furnished = response.xpath("//li[@class='data  meuble']/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//li[@class='data  ASCENSEUR']/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        balcony = response.xpath("//li[@class='data  BALCON']/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        yield item_loader.load_item()