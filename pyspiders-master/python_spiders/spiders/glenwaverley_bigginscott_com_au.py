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

class MySpider(Spider):
    name = 'glenwaverley_bigginscott_com_au' 
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bigginscott.com.au/wp-admin/admin-ajax.php?action=getProperties&sorting=DATE-DESC&category=lease&type=unit%2C&suburbs=&offices=&resultsPerPage=24&page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bigginscott.com.au/wp-admin/admin-ajax.php?action=getProperties&sorting=DATE-DESC&category=lease&type=house%2Ctownhouse%2C&suburbs=&offices=&resultsPerPage=24&page=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["results"]["properties"]:
            seen = True
            yield Request(response.urljoin(item["url"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"], "item":item})
        
        if page == 2 or seen:
            follow_url = response.url.replace("&page=" + str(page - 1), "&page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        if response.url == "https://www.bigginscott.com.au/properties/":
            return
        item_loader.add_value("external_source", "Glenwaverley_Bigginscott_Com_PySpider_australia")
        item = response.meta.get("item")
        item_loader.add_value("title", item["title"])

        if "floor" in item["title"].lower():
            item_loader.add_value("floor", item["title"].lower().split("floor")[0].strip())
        
        room_count = item["features"]["bed"]
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = item["features"]["bath"]
        item_loader.add_value("bathroom_count", bathroom_count)
        
        street = item["address"]["street"]
        city = item["address"]["suburb"]
        if city or street:
            if "Floor" in street:
                item_loader.add_value("address", street.split("Floor /")[-1].split("Floor/")[-1]+" "+city)
            else:
                item_loader.add_value("address", street+" "+city)
            item_loader.add_value("city", city)
            
        zipcode = item["address"]["postcode"]
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            
        rent = item["price"]
        if rent:
            if rent.split(" ")[0].replace(",","").replace(".","").isdigit():
                rent = rent.split(" ")[0].replace(",","")
                item_loader.add_value("rent", int(float(rent))*4)
            else:
                try:
                    price = rent.split("$")[1].lower().replace("pw","").replace("per","").replace("/","").replace("week","").strip().split(" ")[0]
                    item_loader.add_value("rent", int(float(price.replace(",","")))*4)
                except: pass
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent1=response.xpath("//span[@class='price']/text()").get()
            if rent1:
                rent1=re.findall("\d+",rent1.split("-")[0])
                if rent1:
                    item_loader.add_value("rent",rent1) 

                
        item_loader.add_value("currency", "AUD")
        
        item_loader.add_value("external_id", item["property_id"])
        
        images = item["media"]["images"]
        for image in images:
            item_loader.add_value("images", image["url"])
        
        parking = True if int(item["features"]["car"]) > 0 else False
        if parking:
            item_loader.add_value("parking", True)
            
        latitude = item["address"]["location"]["lat"]
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        latitude = item["address"]["location"]["lon"]
        if latitude:
            item_loader.add_value("longitude", latitude)
        
        import dateparser
        available_date = response.xpath("//span[@class='date_available']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//span[@class='bond_amount']/text()").get()
        if deposit:
            deposit = deposit.split("$")[1].replace(",","")
            item_loader.add_value("deposit", int(float(deposit)))
        
        desc = " ".join(response.xpath("//tab-container[@id='property-content-tabs']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        item_loader.add_xpath("landlord_name", "//section[contains(@class,'property-agents')]//li[1]//h3/text()")
        item_loader.add_xpath("landlord_email", "//section[contains(@class,'property-agents')]//li[1]//a[@class='button email-agent-btn']/@data-email")
        item_loader.add_value("landlord_phone","0434 225 155")
        yield item_loader.load_item()