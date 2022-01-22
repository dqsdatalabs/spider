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
    name = 'brucemather_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        yield Request("https://www.brucemather.co.uk/search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms=", 
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='relative']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            rooms = "".join(item.xpath(".//div[@class='itemRooms']//text()").getall())
            yield Request(follow_url, callback=self.populate_item, meta={'rooms': rooms})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Brucemather_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].strip())
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("address", "//h1[@class='fdPropName']/text()")
        item_loader.add_value("zipcode", item_loader.get_collected_values("title")[0].split(',')[-1].strip())
        item_loader.add_value("city", item_loader.get_collected_values("title")[0].split(',')[-2].strip())

        property_type = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return

        rent = "".join(response.xpath("//h2[@class='fdPropPrice']/div/text()").extract())
        if rent:
            item_loader.add_value("rent_string",rent.strip().split(" ")[0].strip())

        deposit = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()[contains(.,'DEPOSIT')]").getall())
        if deposit:
            deposit = deposit.split("DEPOSIT")[1].strip().split(" ")[0].replace("£","").lower().replace("holding","").replace("sorry","").replace("no","")
            item_loader.add_value("deposit", deposit)
        else:
            deposit = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()[contains(.,'Deposit')]").getall())
            if deposit:
                deposit = deposit.split("Deposit")[1].strip().split(" ")[0].replace("£","").lower().replace("holding","").replace("sorry","").replace("no","")
                item_loader.add_value("deposit", deposit)

        room_count = "".join(response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'Bedroom')]]//text()").extract())
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0].strip())

        bathroom_count = "".join(response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'Bathroom')]]//text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0].strip())

        floor = "".join(response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'FLOOR')]]//text()").extract())
        if floor:
            item_loader.add_value("floor",floor.strip().split(" ")[0].strip())

        description = "".join(response.xpath("//div[@class='descriptionsColumn']//text()").extract())
        if description:
            item_loader.add_value("description",description.strip())

        images = [x for x in response.xpath("//div[@class='royalSlider rsDefault visibleNearby']/a[@class='rsImg']/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        terrace = response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'TERRACE')]]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace",False)

        parking = response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'Parking') or contains(.,'parking')]]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)

        furnished = response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'Unfurnished')]]//text()").extract_first()
        if furnished:
            item_loader.add_value("furnished",False)
        elif response.xpath("//div[contains(@class,'detailsFeatures')]/ul/li[span[contains(.,'Furnished')]]//text()").extract_first():
            item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_name", "BRUCE MATHER LTD")
        item_loader.add_value("landlord_phone", "01205 360387")
        item_loader.add_value("landlord_email", "lettings@brucemather.co.uk")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None