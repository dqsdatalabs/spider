# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re
import dateparser
class MySpider(Spider): 
    name = 'yournest_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'     
    def start_requests(self):
        start_urls = [ 
            {
                "url": [
                    "https://www.yournest.co.uk/property/?post_type=property&search_keyword=&locations=&property_type=professional-lets&beds=",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.yournest.co.uk/property/?post_type=property&search_keyword=&locations=&property_type=student-houses&beds=&ex_bills=0",
                ],
                "property_type": "student_apartment"
            },
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
        
        for url in response.xpath("//h3[@class='prop-title']/a/@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_page:            
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "//div[span[.='ID']]/span[2]/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Yournest_Co_PySpider_united_kingdom")
        rent = response.xpath("//span[contains(@class,'prop-price')]/text()").get()
        if rent:
            if "pppw" in rent.lower():
                rent = rent.lower().split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)*4)))    
                item_loader.add_value("currency", "GBP")    
            else:
                item_loader.add_value("rent_string",rent)  
        address = response.xpath("//div[@class='clearfix']//h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            if len(address.split(","))>2:
               item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        desc = "".join(response.xpath("//div/h3[.='Property Description']/following-sibling::p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        desc1 = "".join(response.xpath("//div[@class='clearfix padding030']/div/span/p/text()").getall())
        if desc1:
            item_loader.add_value("description", desc1.strip()) 
        else:
            desc = "".join(response.xpath("//div[@class='clearfix padding030']/div/span/text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip())

        item_loader.add_xpath("room_count", "//div[span[.='Bedrooms']]/span[2]/text()")
        item_loader.add_xpath("bathroom_count", "//div[span[.='Bathrooms']]/span[2]/text()")
        item_loader.add_xpath("title", "//div[@class='clearfix']//h2/text()")
              
        available_date = response.xpath("//div[span[.='Available From']]/span[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        terrace = response.xpath("//li/a/text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//li/a/text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li/a/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
                
        images = [x for x in response.xpath("//div[@id='realto_carousel']//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        
        item_loader.add_value("landlord_email","lettings@yournest.co.uk")
        item_loader.add_value("landlord_phone", "0113 3120 222")
        item_loader.add_value("landlord_name", "Your Nest")
        yield item_loader.load_item()