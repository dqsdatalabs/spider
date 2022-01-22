# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
from word2number import w2n
import re

class MySpider(Spider):
    name = 'carltonestateagents_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    post_url = "http://www.carltonestateagents.co.uk/search_results/"
    current_index = 0
    other_prop = ["Flat"]
    other_prop_type = ["apartment"]
    def start_requests(self):
        formdata = {
            "s_search": "lettings",
            "s_bedrooms": "",
            "s_type": "House",
            "s_minprice": "",
            "s_maxprice": "",
            "Search": "Search",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"house",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id='SearchResu']/div[contains(@class,'PropertyContainer')]"):
            url = item.xpath("./a/@href").get()
            let = item.xpath(".//div[@class='PropertySold']/text()[.='Let']").get()
            if let:
                continue
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if self.current_index < len(self.other_prop):          
            formdata = {
                "s_search": "lettings",
                "s_bedrooms": "",
                "s_type": self.other_prop[self.current_index],
                "s_minprice": "",
                "s_maxprice": "",
                "Search": "Search",
            }
           
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Carltonestateagents_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("details/")[-1].split("/")[0])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//div[@class='PropertyName']/text()[1]").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent = response.xpath("//span[@class='PropertyPrice']/text()").get()
        if rent:
            rent = int(rent.split("£")[1].replace(",",""))*4
            item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "GBP")
        
        description = " ".join(response.xpath("//div[@class='PropertyInfo']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        if "sq ft" in description:
            square_meters = description.split("sq ft")[0].strip().split(" ")[-1]
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)

        room_count = response.xpath("//div[@class='PropertySalesBannerC']/p[contains(.,'bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split("bedroom")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except: pass
        elif "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//div[@class='PropertySalesBannerC']/p[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bathroom")[0].strip().split(" ")[-1])
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='gallery-thumbs-list']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_value("landlord_name", "Carlton Estate Agents")
        item_loader.add_value("landlord_phone", "020 7359 0000")

        yield item_loader.load_item()