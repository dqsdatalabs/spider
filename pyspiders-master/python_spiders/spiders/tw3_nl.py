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
    name = 'tw3_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Tw3_PySpider_netherlands_nl"

    def start_requests(self):
        start_urls = [
            {"url": "https://tw3.nl/aanbod/", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='estatelisting']//div[contains(@class,'overviewblock') and not(contains(@data-type-rent-buy,'Koop'))]"):
            price = int(item.xpath("./@data-from-price").get())
            if price and price > 5000:
                continue
            follow_url = item.xpath("@data-url").extract_first()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
 
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1[contains(@class,'postdetails-title')]/text()")
        dontallow=response.xpath("//td[.='Status']/following-sibling::td/text()").get()
        if dontallow and "Onder optie"==dontallow:
            return 
        external_id=response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id=external_id.split("p=")[-1]
            item_loader.add_value("external_id",external_id)
        
        rent="".join(response.xpath("//table/tbody/tr/td[contains(.,'Prijs')]//following-sibling::td/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//table/tbody/tr/td[contains(.,'Oppervlakte vanaf')]//following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count=response.xpath("//table/tbody/tr/td[contains(.,'Slaapkamers')]//following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        address = response.xpath("//h3[@class='gform_title']/text()").get()
        if address:
            if not any(a for a in address if a.isdigit()):
                address = response.xpath("//h2[contains(@class,'postdetails-city')]/text()").get()
            item_loader.add_value("address", address)

        city = response.xpath("//h2[contains(@class,'postdetails-city')]/text()").get()
        if city:    
            item_loader.add_value("city", city)
        zipcode=response.xpath("//meta[@property='og:site_name']/@content").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("-")[0].strip())

        desc = "".join(response.xpath("//div[@class='wpb_wrapper']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
            
        images=[x for x in response.xpath("//div[contains(@class,'slide blockslide')]/@style").getall()]
        for image in images:
            item_loader.add_value("images", image.split("url('")[1].split("')")[0])
        item_loader.add_value("external_images_count", str(len(images)))
        
        utility = response.xpath("//div[contains(.,'Servicekosten')]/text()").re_first(r'Servicekosten\s€(\d+)')
        if utility:
            item_loader.add_value("utilities", utility)
        utilitycheck=item_loader.get_output_value("utilities")
        if not utilitycheck:
            utilities=response.xpath("//div[contains(.,'Nutsvoorzieningen:')]/text()").re_first(r'Nutsvoorzieningen:\s€\s(\d+)')
            if utilities:
                item_loader.add_value("utilities",utilities)
        

        item_loader.add_value("landlord_name", "TW3")
        location_name = response.xpath("//h2/text()").get()
        phone = "".join(response.xpath("//address//text()").getall())
        if location_name:
            if "ROTTERDAM" in location_name:
                item_loader.add_value("landlord_phone", phone.split("Rotterdam")[1].split("Den Haag")[0].strip())
            else:
                item_loader.add_value("landlord_phone", phone.split("Den Haag")[1].split("Gouda")[0].strip())
        
        item_loader.add_value("landlord_email", "info@tw3.nl")
        
        
        yield item_loader.load_item()