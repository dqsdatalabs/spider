# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import dateparser
class MySpider(Spider):
    name = 'ballaratrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://ballaratrealestate.com.au/?/rent/residential/leased/false',
        'Accept-Language': 'tr,en;q=0.9'
    }

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "http://ballaratrealestate.com.au/?json/listing/restype/5,6/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/60/leaseddays/10",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://ballaratrealestate.com.au/?json/listing/restype/7,9,39,40,11,15/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/60/leaseddays/10",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://ballaratrealestate.com.au/?json/listing/restype/1/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/60/leaseddays/10",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        selector = scrapy.Selector(text=data["BODY"], type="html")
        for item in selector.xpath("//div[contains(@id,'listing-')]/a/@href").getall():
            seen = True
            follow_url = "http://ballaratrealestate.com.au/" + item.replace("\\", "").replace('"', "")
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("page/" + str(page - 1), "page/" + str(page))
            yield Request(follow_url, headers=self.headers, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Ballaratrealestate_Com_PySpider_australia")          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count", "//div[i[@class='fa fa-bed']]/text()[normalize-space()]")
        item_loader.add_xpath("bathroom_count", "//div[i[@class='fa fa-bath']]/text()[normalize-space()]")
        item_loader.add_xpath("external_id", "substring-after(//span[contains(.,'Property ID:')]/text(),': ')")        
        item_loader.add_xpath("deposit", "//span[contains(.,'Bond')]/span/text()")        
        rent = response.xpath("//h4/text()").get()
        if rent:
            rent = rent.split("$")[-1].lower().split('p')[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'USD')
        address = ", ".join(response.xpath("//h1//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.strip())
        item_loader.add_xpath("city", "//h1/strong/text()")
        parking = response.xpath("//div[i[@class='fa fa-car']]/text()[normalize-space()]").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//div[contains(@class,'property-description')]/div/p//text()[contains(.,' balconies')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        available_date = response.xpath("//h5[.='Rental Available Date']/following-sibling::div[1]/div/span/text()").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        description = " ".join(response.xpath("//div[contains(@class,'property-description')]/div/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        
        script_map = response.xpath("//iframe[@class='gmapIFrame']/@src").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split(",")[-1].strip())
            item_loader.add_value("longitude", script_map.split("=")[-1].split(",")[0].strip())
        images = [x for x in response.xpath("//meta[@property='og:image']/@content").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'property-staff')][1]//h3/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'property-staff')][1]//a[i[@class='fa fa-phone']]/span/text()")
        item_loader.add_value("landlord_email", "bre@ballaratrealestate.com.au")
   
        yield item_loader.load_item()