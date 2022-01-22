# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
 
class MySpider(Spider):
    name = 'jfspeersandson_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.jfspeersandson.co.uk/property-for-rent?q=&st=rent&lp=0&up=0&beds=&town=&sta=5&sty=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://www.jfspeersandson.co.uk/property-for-rent?q=&st=rent&lp=0&up=0&beds=&town=&sta=5&sty=8&sty=7&sty=6&sty=5&sty=4&sty=3&sty=2&sty=10"
                ],
                "property_type": "house"
            }
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
        
        for item in response.xpath("//div[@class='properties']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Jfspeersandson_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url.split("/")[-1])
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-1])
        
        title = response.xpath("//span[@class='xPP_briefText']/text()").get()
        item_loader.add_value("title", title)
        
        address = response.xpath("normalize-space(//h1/text())").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            
        rent = response.xpath("//span[contains(@class,'val') and contains(@class,'Rent')]/text()[contains(.,'£')]").get()
        if rent:
            if "week" in rent.lower():
                rent = rent.split(" ")[0].replace("£","")
                item_loader.add_value("rent", int(float(rent))*4)
            else:
                rent = rent.split(" ")[0].replace("£","")
                item_loader.add_value("rent", int(float(rent)))

        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//span[contains(@class,'val') and contains(@class,'deposit')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("£",""))
        
        room_count = response.xpath("//span[contains(@class,'val') and contains(@class,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.replace("£",""))
        
        bathroom_count = response.xpath("//span[contains(@class,'val') and contains(@class,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.replace("£",""))
        
        import dateparser
        available_date = response.xpath("//span[contains(@class,'val') and contains(@class,'available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//span[contains(@class,'val') and contains(@class,'furnished')]/text()").get()
        if furnished and "un" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        description = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='xPhotoViewer_thumbs']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = "".join(response.xpath("//span[contains(@class,'val') and contains(@class,'epc')]//text()").getall())
        if energy_label:
            energy_label = energy_label.split("/")[0].strip()
            item_loader.add_value("energy_label", energy_label)
            
        
        item_loader.add_value("landlord_name", "J.F. Speers & Son")
        item_loader.add_value("landlord_phone", "028 4176 2212")
        item_loader.add_value("landlord_email", "info@jfspeersandson.co.uk")
            
        status = response.xpath("//span[contains(@class,'val') and contains(@class,'Status')]/text()").get()
        if "to let" in status.lower():
            yield item_loader.load_item()