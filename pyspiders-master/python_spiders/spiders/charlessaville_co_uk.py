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
    name = 'charlessaville_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.charlessaville.co.uk/property-lettings/warwickshire/page/1/?address_keyword&property_type=46&maximum_rent&minimum_bedrooms&department=residential-lettings",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.charlessaville.co.uk/property-lettings/warwickshire/page/1/?address_keyword&property_type=33&maximum_rent&minimum_bedrooms&department=residential-lettings",
                    "https://www.charlessaville.co.uk/property-lettings/warwickshire/page/1/?address_keyword&property_type=42&maximum_rent&minimum_bedrooms&department=residential-lettings",
                    "https://www.charlessaville.co.uk/property-lettings/warwickshire/page/1/?address_keyword&property_type=144&maximum_rent&minimum_bedrooms&department=residential-lettings",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//ul[contains(@class,'properties')]/li"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'More Detail')]/@href").get())
            let_agreed = item.xpath(".//div[@class='flag']/text()[contains(.,'Let Agreed')]").get()
            if not let_agreed: yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'next') and contains(@class,'page-numbers')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Charlessaville_Co_PySpider_united_kingdom")
      
        item_loader.add_xpath("title","//div[@class='summary entry-summary']/h1/text()")
        item_loader.add_xpath("room_count","//li[@class='bedrooms']/text()")
        item_loader.add_xpath("bathroom_count","//li[@class='bathrooms']/text()")
        item_loader.add_xpath("deposit","//li[@class='deposit']/text()")
        item_loader.add_xpath("rent_string","//div[@class='price']/text()")
        
        address = response.xpath("//div[@class='summary entry-summary']/h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode =" ".join(address.strip().split(" ")[-2:])
            if not zipcode.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode)

        parking = response.xpath("//li[@class='parking']/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        latitude = response.xpath("//footer//div[@class='gdlr-core-item-pdlr']/a/@href[contains(.,'latitude')]").get()
        if latitude:
            item_loader.add_value("longitude", latitude.split("longitude=")[1].split("&")[0].strip())
            item_loader.add_value("latitude", latitude.split("latitude=")[1].split("&")[0].strip())

        available_date="".join(response.xpath("//li[@class='available-date']/text()").getall())
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
     
        furnished = response.xpath("//li[@class='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
     
        description = " ".join(response.xpath("//div[@class='summary-contents']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        # available_date = response.xpath("//li[@class='available_from']/text()").get()
        # if available_date:
        #     date_parsed = dateparser.parse(available_date.split(":")[-1].replace(" End","").strip(), date_formats=["%d %m %Y"])
        #     if date_parsed:
        #         item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        images = [x.split(",")[0] for x in response.xpath("//ul[@class='slides']/li/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        # script_map = response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        # if script_map:
        #     latlng = script_map.split("google.maps.LatLng(")[1].split(");")[0]
        #     item_loader.add_value("latitude", latlng.split(",")[0].strip())
        #     item_loader.add_value("longitude", latlng.split(",")[1].strip())
        item_loader.add_value("landlord_name", "Charles Saville")
        item_loader.add_value("landlord_phone", "+44(0)1789 293 186")
        item_loader.add_value("landlord_email", "lettings@charlessaville.co.uk")
        yield item_loader.load_item()