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
    name = 'expressrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'expressrealty.com.au',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'referer': 'https://expressrealty.com.au/?/rent/residential/leased/false',
        'accept-language': 'tr,en;q=0.9',
        'Cookie': '__cfduid=db4777473faaf476f5105d795415143411612525869; CFID=46536806; CFTOKEN=916588d11fe1f352-7463A776-C9F0-07BC-551FCEF35EE87657'
    }

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://expressrealty.com.au/?json/listing/restype/5,6/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/60/leaseddays/10",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://expressrealty.com.au/?json/listing/restype/7,9,39,40,11,15/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/60/leaseddays/10",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://expressrealty.com.au/?json/listing/restype/1/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/60/leaseddays/10",
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
            follow_url = "https://expressrealty.com.au/" + item.replace("\\", "").replace('"', "")
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("page/" + str(page - 1), "page/" + str(page))
            yield Request(follow_url, headers=self.headers, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Expressrealty_Com_PySpider_australia")          
        item_loader.add_xpath("title","//h1/span/text()")
        item_loader.add_xpath("room_count", "//div[i[@class='fa fa-bed']]/text()[normalize-space()]")
        item_loader.add_xpath("bathroom_count", "//div[i[@class='fa fa-bath']]/text()[normalize-space()]")
        item_loader.add_xpath("external_id", "substring-after(//span[contains(.,'Property ID:')]/text(),': ')")        
        item_loader.add_xpath("deposit", "//span[contains(.,'Bond')]/span/text()")        
        rent = response.xpath("//h4/text()").get()
        if rent:
            rent = rent.split("$")[-1].lower().split('week')[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
        address = ", ".join(response.xpath("//h1//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.strip())

        zipcode = response.xpath("//meta[@name='Description']/@content").get()
        if zipcode:
            item_loader.add_value("zipcode", f"NSW {zipcode.split(' ')[-1]}")

        item_loader.add_xpath("city", "//h1/strong/text()[not(contains(.,'Beach'))]")
        parking = response.xpath("//div[i[@class='fa fa-car']]/text()[normalize-space()]").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//div[contains(@class,'property-description')]/div/p//text()[contains(.,' balconies')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        available_date = response.xpath("//h5[.='Rental Available Date']/following-sibling::div[1]/div/span/text()").get()
        if available_date:
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
      
        item_loader.add_value("landlord_name", "Express Realty")
        item_loader.add_value("landlord_phone", "02 9365 7799")
        item_loader.add_value("landlord_email", "enquiries@expressrealty.com.au")

        yield item_loader.load_item()