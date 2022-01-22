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
    name = 'bradfieldcleary_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        # url = "https://bradfieldcleary.com.au/BradfieldClearyWebsite_272/38-54a-hopewell-street-paddington"
        # yield Request(url, callback=self.populate_item ,meta={"property_type":"apartman"})
        start_url = "https://bradfieldcleary.com.au/properties-for-lease?category=residential&page=1"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[contains(@class,'searchResult')]/li"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            property_type = item.xpath("./@class").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

        next_button = response.xpath("//a[@class='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Bradfieldcleary_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("//h2/small/text()").extract())
        if rent:
            price =  rent.split("p")[0].split("$")[1].replace(",","").strip()
            item_loader.add_value("rent",int(float(price))*4)
        item_loader.add_value("currency","USD")

        room_count = "".join(response.xpath("//h1[@class='text-center']/div/i[@class='icon-bed']/preceding-sibling::text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = "".join(response.xpath("//h1[@class='text-center']/div/i[@class='icon-bath']/preceding-sibling::text()[1]").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())


        address = " ".join(response.xpath("//h1[@class='text-center']/span//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_xpath("city", "substring-after(//title/text(),', ')")

        item_loader.add_xpath("latitude","substring-before(substring-after(//script[@type='text/javascript']//text()[contains(.,'LatLng')],'LatLng('),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(substring-after(//script[@type='text/javascript']//text()[contains(.,'LatLng')],'LatLng('),','),')')")

        desc =  " ".join(response.xpath("//div[@class='contentRegion text-justify']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date="".join(response.xpath("//div[@class='column tiny-12 med-6 lrg-4 push-lrg-1 highlight']/div/div/text()").getall())
        if available_date:
            date2 =  available_date.strip().replace("Immediate","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        img = "".join(response.xpath("//style//text()").extract())
        if img:
            im = img.split("@media only screen and (min-width: 1024px)")[1].split("*{}")[0].strip().split("{")[1:]
            for i in im:
                if "url" in i:
                    image =  [i.strip().split("url(")[1].split(")")[0].strip()]
                    item_loader.add_value("images", image) 

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplanpanel']//a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
                

        parking = response.xpath("//h1[@class='text-center']/div/i[@class='icon-car']/preceding-sibling::text()[1]").get()
        if parking:
            if int(parking.strip()) > 0: item_loader.add_value("parking", True)
            else: item_loader.add_value("parking", False)

        item_loader.add_xpath("landlord_name", "//div[@class='agentName']//span/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='oneline']/a//span[@itemprop='telephone']//text()")
        item_loader.add_xpath("landlord_email", "substring-after(//div[@class='oneline']/a/@href[contains(.,'mailto')],':')") 

    

        
   
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None