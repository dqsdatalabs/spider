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
from word2number import w2n

class MySpider(Spider):
    name = 'kavanaghs_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Kavanaghs_Co_PySpider_united_kingdom"

    def start_requests(self):
        yield Request("https://kavanaghs.co.uk/residential-lettings/view-properties/", callback=self.parse)

    def parse(self, response):

        for item in response.xpath("//div[@class='properties']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            property_type = item.xpath(".//h5/text()").get()
            let_agreed = item.xpath(".//div[@class='photo__corner' and contains(.,'Let Agreed')]").get()
            if get_p_type_string(property_type) and not let_agreed:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

        next_button = response.xpath("//a[@class='nextlink']/@href").get()
        if next_button:
            yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("latitude", "substring-before(substring-after(//div[@id='propertydirections']/a/@href,'@'),',')")
        item_loader.add_xpath("longitude", "substring-after(//div[@id='propertydirections']/a/@href,',')")

        rent = response.xpath("substring-before(//div[@class='intro intro--property']/h4/text(),' ')").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.split())

        address = "".join(response.xpath("//div[@class='intro intro--property']/h5/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())

        floor = "".join(response.xpath("//div[@class='intro__features']/ul/li[contains(.,'Floor')]/text()").extract())
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0].split())

        description = "".join(response.xpath("//div[@class='intro intro--sub']/p/text()").extract())
        if description:
            item_loader.add_value("description", description.strip())

        energy_label = "".join(response.xpath("//div[@class='intro__features']/ul/li[contains(.,'EPC ')]/text()").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[1].strip())

        room_count = "".join(response.xpath("//div[@class='intro__features']/ul/li[contains(.,'Bedroom')]/text()").extract())
        if room_count:
            room = room_count.split(" ")[0].strip()
            if "Double" not in room and room:        
                
                try:
                    number = w2n.word_to_num(room)
                    item_loader.add_value("room_count",number)
                except:
                    pass

        bathroom_count = response.xpath("//div[@class='intro__features']/ul/li[contains(.,'Bathroom')]/text()").extract_first
        if bathroom_count:
            item_loader.add_value("bathroom_count","1")

        images = [x for x in response.xpath("//div[@class='property']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date=response.xpath("//div[@class='intro__features']/ul/li[contains(.,'Available ')]/text()[.!='Available NOW']").get()
        if available_date:
            
            date2 =  available_date.replace("Available:","").replace("Available","").replace("Mid","").replace("End of","").replace("Early","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        parking = "".join(response.xpath("//div[@class='intro__features']/ul/li[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        furnished = "".join(response.xpath("//div[@class='intro__features']/ul/li[contains(.,'furnished')]/text()").extract())
        if furnished:
            if furnished == "Unfurnished":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Kavanaghs Estate Agents")
        item_loader.add_value("landlord_phone", "01225 790529")
        item_loader.add_value("landlord_email", "lettingsteam@kavanghs.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None