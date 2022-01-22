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
from python_spiders.helper import ItemClear


class MySpider(Spider):
    name = 'rhapsodyuk_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://rhapsodyuk.com/professional-lettings/search?type=7&price-min=280&price-max=2300&sort_by=",
                    "http://rhapsodyuk.com/professional-lettings/search?type=11&price-min=280&price-max=2300&sort_by=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://rhapsodyuk.com/professional-lettings/search?type=15&price-min=280&price-max=2300&sort_by=",
                    "http://rhapsodyuk.com/professional-lettings/search?type=3&price-min=280&price-max=2300&sort_by=",
                    "http://rhapsodyuk.com/professional-lettings/search?type=12&price-min=280&price-max=2300&sort_by=",
                    "http://rhapsodyuk.com/professional-lettings/search?type=13&price-min=280&price-max=2300&sort_by=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://rhapsodyuk.com/professional-lettings/search?type=4&price-min=280&price-max=2300&sort_by=",
                ],
                "property_type" : "room",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='thumb']"):
            status = item.xpath("./a/div/span/text()").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"&page={page}&is_ajax=1"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "base_url":base_url, "property_type":response.meta["property_type"]})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Rhapsodyuk_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url.split("?")[0])

        title = "".join(response.xpath("//div[@class='fl']//text()").getall())
        if title:
            item_loader.add_value("title", title.replace("\n","").replace("\t","").strip())
        
        address = "+".join(response.xpath("//div[@class='fl']//text()").getall())
        if address:
            item_loader.add_value("address", address.replace("\n","").replace("\t","").split(",")[0].replace("+","").strip())
            item_loader.add_value("city", address.split(",")[0].split("+")[-1].strip())
            item_loader.add_value("room_count", address.split(",")[-1].strip().split(" ")[0])

        desc = " ".join(response.xpath("//div[contains(@class,'information')]/*[self::p and not(self::section) or self::ul]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta.get('property_type'))
                    
        bathroom_count = response.xpath("//div/h3[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.capitalize())
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='fr']/h2/text()", input_type="F_XPATH", get_num=True, split_list={".":0, "£":1})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'EPC')]/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//td/p[contains(.,'Security Deposit')]/parent::td/following-sibling::td//text()", input_type="F_XPATH", split_list={".":0}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(.,'Ref')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div/h3[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div/h3[contains(.,'Parking') or contains(.,'parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="M_XPATH", split_list={'lat:"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="M_XPATH", split_list={'lng:"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="RHAPSODY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="44 0191 281 8758", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@rhapsodyuk.com", input_type="VALUE")

        import dateparser
        available_date = response.xpath("//div/h3[contains(.,'Available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed: item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath("//h3[contains(.,'Shower room')]/text()").get()
            if bathroom_count: item_loader.add_value("bathroom_count", "".join(filter(str.isnumeric, bathroom_count)))

        utilities = " ".join(response.xpath("//div[contains(@class,'information')]/*[self::p and not(self::section) or self::ul]//text()").getall())
        if utilities:
            if "Utility" in utilities:
                utilities = utilities.split("Utility")[0].split("£")[-1]
                item_loader.add_value("utilities", utilities)
           
        yield item_loader.load_item()