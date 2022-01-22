# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'coppolaimmobiliare_com'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Coppolaimmobiliare_PySpider_italy"
    start_urls = ['https://www.coppolaimmobiliare.com/risultati-ricerca/?lang=en']  # LEVEL 1

    formdata = {
        "location_level1": "",
        "rentbuy": "rent",
        "beds": "",
        "baths": "",
        "minprice_buy": "0",
        "maxprice_buy": "9999999999999",
        "minprice_rent": "0",
        "maxprice_rent": "9999999999999",
        "propertytype2": "",
    }
    
    def start_requests(self):
        yield FormRequest(
            url=self.start_urls[0],
            formdata=self.formdata,
            callback=self.parse,
        )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='listingblocksection']"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Detail')]/@href").get())
            prop_type = item.xpath(".//p[@class='twofeatures']/text()").get()
            property_type = ""
            if (prop_type) and ("holiday" not in prop_type.lower()):
                print(prop_type.lower(),"--")
                if "loft" in prop_type.lower() or "house" in prop_type.lower() or "villa" in prop_type.lower():
                    property_type = "house"
                    
                elif "apartment" in prop_type.lower():
                    property_type = "apartment"
                elif "studio" in prop_type.lower():
                    property_type = "studio"
                else:
                    property_type = "apartment"
            if property_type:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)                
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        rent=response.xpath("//h2[@id='pricebig']//text()").get()
        if rent and (not rent.isalpha()):
            rent = re.search("([\d.]+)",rent)
            if rent:
                rent = rent.group(1)
                rent = rent.replace(".","")
                if float(rent) > 20000:
                    rent = int(rent)//100
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        desc=response.xpath("//div[@id='listingcontent']//p//text()").getall()
        if desc:
            item_loader.add_value("description",desc)
            
        bathroom_count=response.xpath("//ul[@class='specslist']//li[contains(.,'Bathrooms')]//text()").get()
        if bathroom_count:
            bathroom_count=bathroom_count.split("Bathrooms:") [1]
            item_loader.add_value("bathroom_count",bathroom_count)

        square_meters=response.xpath("//li[contains(.,'Size:')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("Size:")[1].split("Sq")[0])

        images = [response.urljoin(x) for x in response.xpath("//a[@rel='prettyPhoto[pp_gal]']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Coppola Immobiliare")
        item_loader.add_value("landlord_phone", "+39 081 804608")
        item_loader.add_value("landlord_email", "info@coppolaimmobiliare.com")

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value("external_id", external_id)

        
        property_type = response.xpath("//ul[contains(@class,'specslist')]/li[2]/text()").get()
        if property_type:
            property_type=property_type.split(",")[0].strip()
            item_loader.add_value("property_type",property_type)

        city = response.xpath("//h3[@class='detailpagesubheading']/text()").get()
        if city:
            item_loader.add_value("city",city.strip())

        room_count = response.xpath("//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split(":")[-1].strip()
            item_loader.add_value("room_count",room_count)

        address = response.xpath("//h2[@id='title']/text()").get()
        if address:
            item_loader.add_value("address",address.strip())

        yield item_loader.load_item()