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
import dateparser

class MySpider(Spider):
    name = 'hockeys_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://hockeys.com.au/properties-for-lease?ac=&min=0&max=999999999&orderby=&type%5B%5D=apt&type%5B%5D=flt&type%5B%5D=unt&searchtype=2&map=&view=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://hockeys.com.au/properties-for-lease?ac=&min=0&max=999999999&orderby=&type%5B%5D=dup&type%5B%5D=hou&type%5B%5D=ter&type%5B%5D=tow&type%5B%5D=vil&searchtype=2&map=&view=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://hockeys.com.au/properties-for-lease?ac=&min=0&max=999999999&orderby=&type%5B%5D=stu&searchtype=2&map=&view=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            dont_filter=True,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[@class='searchResults']/ul/li//a[@class='propertyTile-anchor']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@class='sd next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("au/")[1].split("/")[0].strip())
        item_loader.add_value("property_type", response.meta["property_type"])

        item_loader.add_value("external_source", "Hockeys_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("city", "//h1[@class='page-headline']/address/span[@class='suburb block']/text()")

        address = "".join(response.xpath("//h1[@class='page-headline']/address//text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        rent = response.xpath("//div[@class='muted']/text()").extract_first()
        if rent:
            if "Deposit" in rent or "Contact" in rent:
                return
            else:
                price = rent.split(" ")[0].strip().replace("$","").strip().replace("pw","")
                if price.isdigit():
                    item_loader.add_value("rent",int(price)*4)
        item_loader.add_value("currency","AUD")

        deposit = response.xpath("//div[contains(@class,'tiny-12')]/div/b[.='Bond:']/following-sibling::text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",deposit.strip())

        available_date="".join(response.xpath("//div[contains(@class,'tiny-12')]/div/b[.='Availability:']/following-sibling::text()").getall())
        if available_date:
            date2 =  available_date.strip()
            if "Now" not in date2:
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//div[contains(@class,'image')]/span/link/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("latitude", "//meta[@property='place:location:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='place:location:longitude']/@content")

        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floorplan')]/span/link/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        description = "".join(response.xpath("//div[@class='column tiny-12 med-7']/div/p/text()").extract())
        if description:
            item_loader.add_value("description",description.strip())


        room = "".join(response.xpath("//div[contains(@class,'icons')]/i[@class='icon-bed']/preceding-sibling::text()[1]").extract())
        if room:
            item_loader.add_value("room_count",room.strip())
        else:
            item_loader.add_value("room_count","1")
        
        bathroom_count = "".join(response.xpath("//div[contains(@class,'icons')]/i[@class='icon-bath']/preceding-sibling::text()[1]").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        parking = "".join(response.xpath("//li[@class='kvgroup']/span[contains(.,'Garage') or contains(.,'Parking')]").extract())
        if parking:
            item_loader.add_value("parking",True)

        balcony = "".join(response.xpath("//ul[@class='clear-both']/li/span[.='Balcony']/text()").extract())
        if balcony:
            item_loader.add_value("balcony",True)

        dishwasher = "".join(response.xpath("//ul[@class='clear-both']/li/span[.='Dishwasher']/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher",True)

        pets_allowed = "".join(response.xpath("//li[@class='kvgroup']/span[contains(.,'Pet ')]").extract())
        if pets_allowed:
            item_loader.add_value("pets_allowed",True)

        item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='agentTile-name']/text())")
        item_loader.add_xpath("landlord_phone", "//li[@class='agentTile-contact-phone']/a//text()")
        item_loader.add_xpath("landlord_email", "normalize-space(//li[@class='agentTile-contact-email']/a/text())")
        
        yield item_loader.load_item()