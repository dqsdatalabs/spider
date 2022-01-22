# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'maxxhuren_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source = "Maxx_PySpider_netherlands"
    def start_requests(self):
        start_urls = [ 
            {
                "url" : [
                    "https://maxxhuren.nl/objects/objects/search/type-Appartement/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://maxxhuren.nl/objects/objects/search/type-Benedenwoning/",
                    "https://maxxhuren.nl/objects/objects/search/type-Bovenwoning/",
                    "https://maxxhuren.nl/objects/objects/search/type-Eengezinswoning/"
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://maxxhuren.nl/objects/objects/search/type-Kamer/",
                ],
                "property_type" : "room",
            },
            {
                "url" : [
                    "https://maxxhuren.nl/objects/objects/search/type-Studio/",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='box-object']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("id-")[-1].split("/")[0])
        item_loader.add_xpath("title", "//h2/text()")
        item_loader.add_xpath("room_count", "//i[@class='fas fa-bed iconLarge']/following-sibling::text()[1]")
        dontallow=response.xpath("//h5//text()").get()
        if dontallow=="Verhuurd" or dontallow=="Aangehuurd":
            return 

        rent =  response.xpath("//h3[@class='text-red price']/text()").get()
        if rent:
            price =  rent.split(",")[0].strip().replace(".","")
            item_loader.add_value("rent_string", price)
     
        square_meters=response.xpath("//i[@class='fas fa-arrows-alt iconLarge']/following-sibling::text()[1]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        address = ", ".join(response.xpath("//div[@class='col-8 col-md-3 p-3']/strong/text()[normalize-space()]").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        city = " ".join(response.xpath("//div[@class='col-8 col-md-3 p-3']/strong/text()[last()]").getall())
        if city:
            city1=re.findall("[A-Z][A-Z]",city)
            zipcode =  city.split(" ")[0].strip()
            
            if not zipcode.isdigit():
                item_loader.add_value("zipcode",zipcode)
            else:
                item_loader.add_value("zipcode",zipcode+city1[0])

            item_loader.add_value("city", city.replace(zipcode,"").strip())
   
        desc = "".join(response.xpath("//div[@id='gallery']/following-sibling::div/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='gallery']//a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 
            
        phone = " ".join(response.xpath("//div[@id='locaties']/div[1]//a[@class='btn btn-phone']//text()").getall()).strip()   
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())

        item_loader.add_xpath("landlord_name", "//div[@id='locaties']/div[1]//h3/text()")
        item_loader.add_value("landlord_email", "info@maxxzwolle.nl")
   
        yield item_loader.load_item()