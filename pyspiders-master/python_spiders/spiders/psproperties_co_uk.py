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
    name = 'psproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://psproperties.co.uk/property-search/department/residential-lettings/"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[contains(@class,'properties')]/li"):
            status = item.xpath("./div[@class='flag']/text()").get()
            if status and "let agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//p[@class='room']/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        item_loader.add_value("external_source", "Psproperties_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//h1/text()")
                   
        description = " ".join(response.xpath("//div[@class='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        room_count = response.xpath("//img[contains(@src,'bed-icon')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//img[contains(@src,'bathroom-icon')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('pcm')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='carousel']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'property_map')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip())

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        elif response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]").get():
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Property Solutions")
        item_loader.add_value("landlord_phone", "01280 821799")
        item_loader.add_value("landlord_email", "lettings@psproperties.co.uk")
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None