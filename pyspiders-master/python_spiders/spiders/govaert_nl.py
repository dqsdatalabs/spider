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
import re

class MySpider(Spider):
    name = 'govaert_nl'
    start_urls = ['https://www.govaert.nl/verhuur/huis-huren/'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Govaert_PySpider_netherlands_nl'
 # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'inner33 omega3')]"):
            follow_url = item.xpath("./a/@href").extract_first()
            prop_type = " ".join(item.xpath("./div[@class='info']/text()").extract()).strip().split(" ")[0]

            if "appartement" in prop_type:
                prop_type = "apartment"
            elif ("tussenwoning" or "2-onder-1-kapwoning" or "vrijstaande" or "hoekwoning" or "villa") in prop_type:
                prop_type = "house"
            else:
                prop_type = None

            yield Request(follow_url, callback=self.populate_item, meta={"prop_type":prop_type})
        
        next_page = response.xpath("//a[contains(.,'volgende')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Govaert_PySpider_" + self.country + "_" + self.locale)
        
        prop_type = response.meta.get("prop_type")
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        
        title = response.xpath("////h2[@id='mapsName']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[@class='entry-content']/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        price = response.xpath("//table[@class='info']//tr[./th[.='Prijs']]/td/text()").get()
        if price:
            rent = price.split("€")[1].split(" ")[0].split(",")[0].replace(".","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

        square = response.xpath(
            "//table[@class='info']//tr[./th[.='Woonoppervlakte']]/td/text()"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m")[0]
            )
        room_count = response.xpath("//th[contains(.,'kamers')]/following-sibling::td/text()").re_first(r'\d+')
        if room_count:
            item_loader.add_value("room_count", room_count)
       
        deposit = response.xpath("//text()[contains(.,'Waarborgsom') and contains(.,'-') and contains(.,'maand huur') and contains(.,'€')]").get()
        if deposit: 
            item_loader.add_value("deposit", deposit.split("€")[-1].split(',')[0].strip())
        else:
            deposit = response.xpath("//text()[contains(.,'Waarborgsom') and contains(.,'maand huur')]").get()
            if deposit:
                deposit = "".join(filter(str.isnumeric, deposit))
                if deposit.isnumeric(): item_loader.add_value("deposit", str(int(rent) * int(deposit)))

        address = response.xpath("//table[@class='info']//tr[./th[.='Adres']]/td/text()").get()
        item_loader.add_value("address", address)
        item_loader.add_value("city", split_address(address, "city"))

        address = address.split(" ")
        for i in address:
            if i.isdigit():
                item_loader.add_value("zipcode", i)
                break
        
        date2 = ""
        available_date=response.xpath("substring-after(//div[@class='entry-content']/p/text()[contains(.,'Beschikbaa')],'per ')").get()
        if available_date:
            if "(" in available_date:
                date2 = available_date.split("(")[0].strip()
            else:
                date2 =  available_date.replace(";","").replace("ca.","").replace("direct","now").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        utilities = response.xpath("//th[contains(.,'Servicekosten')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip().split(' ')[0])
        else:
            item_loader.add_value("utilities", "substring-before(substring-after(//div[@class='entry-content']/p/text()[contains(.,'servicekosten')],'€ '),',')")

        balcony = response.xpath("//th[contains(.,'Overige ruimten')]/following-sibling::td/text()[contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        item_loader.add_xpath("energy_label", "//table[@class='info']//tr[./th[.='Energielabel klasse']]/td/text()")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//li[@class='gfield']/a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", list(set(images)))
        
        
        item_loader.add_value("landlord_phone", "033-463 0444")
        item_loader.add_value("landlord_name", "Govaert")
        item_loader.add_value("landlord_email", "oginfo@govaert.nl")

        yield item_loader.load_item()

def split_address(address, get):
    if "," in address:
        city = address.split(",")[1]

        return city.strip()
