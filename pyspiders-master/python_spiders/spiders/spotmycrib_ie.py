# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'spotmycrib_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.spotmycrib.ie/b/apartment-for-rent-in-ireland/1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.spotmycrib.ie/b/house-for-rent-in-ireland/1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.spotmycrib.ie/b/student-for-rent-in-ireland",
                ],
                "property_type" : "student apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//section[contains(@class,'property-list')]//div[@class='row']/div//a[contains(.,'view details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'Next') and not(contains(@href,'javascript:void'))]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Spotmycrib_PySpider_ireland")
        item_loader.add_xpath("title","//title/text()")
        address = "".join(response.xpath("//li[span[.='Location']]/h5/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        room_count = " ".join(response.xpath("//h5[@class='heading-reg'][contains(.,'Bedrooms :')]/text()").getall())
        if room_count:                   
            item_loader.add_value("room_count",room_count.split("Bedrooms :")[1].strip())
        bathroom_count = response.xpath("//div[@class='purva']/p/text()[contains(.,'baths')]/preceding-sibling::text()[1]").get()
        if bathroom_count:                   
            item_loader.add_value("bathroom_count",bathroom_count)
        rent = "".join(response.xpath("//div[@class='purva-developer']/h5/text()").getall())
        if rent:       
            if "weekly" in rent:
                rent = rent.split('€')[-1].split('(')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'EUR')
            else:       
                item_loader.add_value("rent_string", rent) 
        
    
        description = " ".join(response.xpath("//div[@class='mar-top-20 ' and h5[.='Description']]/p//text()").getall())   
        if description:
            item_loader.add_value("description", description.strip())
        else:
            description = " ".join(response.xpath("//div[@class='purva']/p/text()[not(contains(.,'Click apply below'))]").getall())  
            if description:
                item_loader.add_value("description", description.strip())
        desccheck=item_loader.get_output_value("description")
        if not desccheck:
            return 

        available_date = response.xpath("//li[h3[.='Available from']]/h4/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@class='carousel-inner']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//div[@class='purva']/p/text()[contains(.,'furnished') or contains(.,'unfurnished') or contains(.,'Unfurnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        parking = response.xpath("//div[@class='amenitiy-block']//li/p[contains(.,'Parking')]/text()").get()
        if parking:                   
            item_loader.add_value("parking", True)
        balcony = response.xpath("//div[@class='amenitiy-block']//li/p[contains(.,'balcony')]/text()").get()
        if balcony:                   
            item_loader.add_value("balcony", True)
        washing_machine = response.xpath("//div[@class='amenitiy-block']//li/p[contains(.,'Washing machine')]/text()").get()
        if washing_machine:                   
            item_loader.add_value("washing_machine", True)
        dishwasher = response.xpath("//div[@class='amenitiy-block']//li/p[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:                   
            item_loader.add_value("dishwasher", True)
        pool = response.xpath("//div[@class='amenitiy-block']//li/p[contains(.,'Swimming pool')]/text()").get()
        if pool:                   
            item_loader.add_value("swimming_pool", True)
        pets = response.xpath("//div[@class='amenitiy-block']//li/p[contains(.,'Pets allowed')]/text()").get()
        if pets:                   
            item_loader.add_value("pets_allowed", True)
  
        landlord_name = " ".join(response.xpath("//div[@class='col-md-3' and h5[.='Contact']]//div[@class='col-md-12']//h3/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.replace(":","").strip())
        else:
            item_loader.add_value("landlord_name", "SpotMyCrib")

        item_loader.add_xpath("landlord_phone", "//div[@class='col-md-3' and h5[.='Contact']]//div[@class='col-md-12']//a/text()")
        item_loader.add_value("landlord_email", "support@spotmycrib.com")

        yield item_loader.load_item()