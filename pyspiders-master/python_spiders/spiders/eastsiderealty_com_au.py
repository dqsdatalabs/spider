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
    name = 'eastsiderealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 YaBrowser/20.12.3.140 Yowser/2.5 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'tr,en;q=0.9',
            'Cookie': 'ASP.NET_SessionId=4iuux15bisn4sq51vdf4qwxr'
        }

        start_urls = [
            {
                "url" : [
                    "http://eastsiderealty.com.au/search/results.aspx?class=6&renttype=res&propdesc=a,f&order=s&agtid=1000879",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://eastsiderealty.com.au/search/results.aspx?class=6&renttype=res&propdesc=d,h,s,twn,v&order=s&agtid=1000879",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://eastsiderealty.com.au/search/results.aspx?class=6&renttype=res&propdesc=stu&order=s&agtid=1000879",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            headers=headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='results-encasing']//a[contains(.,'Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(.,'»')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Eastsiderealty_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        address = "".join(response.xpath("//h1//span[@itemprop='address']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            
        city = response.xpath("//h1//span[@itemprop='addressLocality']//text()").get()
        if city:
            item_loader.add_value("city", city)
            
        zipcode = " ".join(response.xpath("//h1//span[@itemprop='addressRegion']//text() | //h1//span[@itemprop='postalCode']//text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            
        rent = response.xpath("//span[@itemprop='price']//text()").get()
        if rent:
            rent = rent.split(" ")[0].strip().replace("$","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")
        
        if response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_xpath("room_count", "//tr/td[contains(.,'Bedroom')]/following-sibling::td/text()")
            
        item_loader.add_xpath("bathroom_count", "//tr/td[contains(.,'Bathroom')]/following-sibling::td/text()")
        
        external_id = response.xpath("//div[contains(.,'No:')]/span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        import dateparser
        available_date = response.xpath(
            "//tr/td[contains(.,'Available')]/following-sibling::td/text()[.!='0']"
        ).get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        item_loader.add_xpath("latitude", "//input[contains(@id,'lat')]/@value[.!='0']")
        item_loader.add_xpath("longitude", "//input[contains(@id,'lng')]/@value[.!='0']")
        
        desc = " ".join(response.xpath("//div[contains(@id,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
         
        images = [x for x in response.xpath("//div[@id='galleria']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        parking = response.xpath("//tr/td[contains(.,'Parking')]/following-sibling::td/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","").replace("rd","").replace("st","").replace("nd","")
            if floor.replace(")","").replace("(","").isdigit():
                item_loader.add_value("floor", floor.replace("-","").replace("(",""))
        
        
        item_loader.add_xpath("landlord_name", "//dl/dt[contains(.,'Name')]/following-sibling::dd[1]/text()")
        item_loader.add_value("landlord_phone", "(02) 9314 7955")
        item_loader.add_value("landlord_email", "leasing@eastsiderealty.com.au")
        
        yield item_loader.load_item()