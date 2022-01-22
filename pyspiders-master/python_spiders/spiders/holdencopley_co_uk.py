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
    name = 'holdencopley_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Flat",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Apartment",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Apartment+-+Retirement",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Flat+-+Penthouse"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=House+-+Detached",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=House+-+Terraced",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Bungalow+-+Detached",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Bungalow+-+Semi+Detached",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=House+-+Townhouse",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=House+-+Semi-Detached",
                    "https://www.holdencopley.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=House+-+End+Terrace"


                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='property-image']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})      
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Holdencopley_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])
        
        address = "".join(response.xpath("//div[contains(@class,('col-md-8'))]/h1/text()").getall())
        if address:
            zipcode = address.strip().split(",")[-1].strip()
            city = address.strip().split(",")[-2].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        rent = "".join(response.xpath("//span[@itemprop='price']/@content").getall())
        if rent:
            price = rent.strip().replace(",",".").replace(".","")
            item_loader.add_value("rent",price.strip())
        item_loader.add_value("currency","GBP")

        room_count = "".join(response.xpath("//span[@class='hidden-xs']/span[contains(@class,'res-bed')]/text()[.!='0']").getall())
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count = "".join(response.xpath("//span[@class='hidden-xs']/span[contains(@class,'res-bath')]/text()[.!='0']").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        description = " ".join(response.xpath("//div[@id='property-long-description']/div/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [response.urljoin(x) for x in response.xpath("//a[@data-target='#property-carousel']/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='property-floorplans']/img/@src").extract()]
        if floor_plan_images is not None:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        available_date=response.xpath("//div[@class='property-list hidden-xs']/ul/li[contains(.,'AVAILABLE')]/text()[1]").get()
        if available_date:
            date2 =  available_date.split("AVAILABLE")[1].replace("!","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        parking = "".join(response.xpath("//div[@class='property-list hidden-xs']/ul/li[contains(.,'PARKING')]/text()[1]").getall())
        if parking:
            item_loader.add_value("parking",True)

        terrace = "".join(response.xpath("//div[@class='property-list hidden-xs']/ul/li[contains(.,'TERRACED')]/text()[1]").getall())
        if terrace:
            item_loader.add_value("terrace",True)

        LatLng = "".join(response.xpath("substring-before(substring-after(//script[contains(.,'googlemap')]/text(),'(opt, '),',')").getall())
        if LatLng:
            item_loader.add_value("latitude",LatLng.strip())
            lng = "".join(response.xpath("substring-before(substring-after(substring-after(//script[contains(.,'googlemap')]/text(),'(opt, '),', '),',')").getall())
            item_loader.add_value("longitude",lng.strip())

        item_loader.add_value("landlord_phone", "0115 8969800")
        item_loader.add_value("landlord_email", "info@holdencopley.co.uk")
        item_loader.add_value("landlord_name", "Holden Copley")
          
        yield item_loader.load_item()