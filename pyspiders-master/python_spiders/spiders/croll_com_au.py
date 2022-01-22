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
    name = 'croll_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Croll_Com_PySpider_australia'
    def start_requests(self):
        start_url = "https://www.croll.com.au/dbpage.php?pg=results&NavPrev=%3C+Prev+20&NavPage1=2&NavCurrent=2&pg=results&Country=Australia&Category=Rental%7CShortRent&Active=Yes"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='frame']/ul/div//h3/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        last_page = response.xpath("//input[@name='NavNext' and @disabled]").get()
        if not last_page:
            queries = {}
            queries["pg"] = response.xpath("//tr[@class='navback']//input[@name='pg']/@value").get()
            queries["NavPage1"] = response.xpath("//tr[@class='navback']//select[@name='NavPage1']/option[@selected]/@value").get()
            queries["NavNext"] = response.xpath("//tr[@class='navback']//input[@name='NavNext']/@value").get()
            queries["NavCurrent"] = response.xpath("//tr[@class='navback']/../input[@name='NavCurrent']/@value").get()
            queries["Country"] = response.xpath("//tr[@class='navback']/../input[@name='Country']/@value").get()
            queries["Category"] = response.xpath("//tr[@class='navback']/../input[@name='Category']/@value").get()
            queries["Active"] = response.xpath("//tr[@class='navback']/../input[@name='Active']/@value").get()
            from urllib.parse import urlencode
            params = urlencode(queries)
            yield Request("https://www.croll.com.au/dbpage.php?" + params, callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        shop = response.xpath("//div[@class='housing-list info-list']/span[@class='bed']//text()").extract_first()
        if shop  is None:
            return
        property_type = "".join(response.xpath("//div[@class='meta-section']/following-sibling::div/div/text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("listing-")[1].split("-")[0])
        item_loader.add_xpath("title", "//title/text()")

        rent =" ".join(response.xpath("substring-after(//div[@class='meta']/text(),': ')").extract())
        if rent:
            if "$" in rent:
                price =  rent.replace("From","").strip().split(" ")[0].split("$")[1].replace(",","").strip()
                item_loader.add_value("rent",int(float(price))*4)
            elif rent.isdigit(): item_loader.add_value("rent", rent)
        item_loader.add_value("currency","AUD")

        item_loader.add_xpath("room_count","//div[@class='housing-list info-list']/span[@class='bed']//text()")
        item_loader.add_xpath("bathroom_count","//div[@class='housing-list info-list']/span[@class='bathroom']//text()")


        address = " ".join(response.xpath("//div[@class='nfw-span9 house-info']/h2//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_xpath("city","//div[@class='nfw-span9 house-info']/h2/a/text()")

        desc =  " ".join(response.xpath("//div[@style='margin-bottom: 15px; padding-right: 15px;']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x for x in response.xpath("//ul[@class='thumbnails']/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [ response.urljoin(x) for x in response.xpath("//div[@class='property-user-links']/a[@id='floorplan']/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images) 

        item_loader.add_xpath("latitude","substring-before(substring-after(//script[contains(.,'latitude')]/text(),'latitude: '),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(//script[contains(.,'latitude')]/text(),'longitude: '),',')")

        deposit = "".join(response.xpath("//div[@class='nfw-span9 house-info']/div/text()[contains(.,'Bond')]").extract())
        if deposit:
            dep =  deposit.split(":")[1].strip()
            item_loader.add_value("deposit",dep)



        available_date="".join(response.xpath("//div[@class='nfw-span9 house-info']/div/text()[contains(.,'Available')]").getall())
        if available_date:
            date2 =  available_date.split(":")[1].strip().replace("Immediate","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        dishwasher = "".join(response.xpath("//ul/li[contains(.,'Dishwaster')]/text()").extract())      
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        swimming_pool = "".join(response.xpath("//ul/li[contains(.,'Swimming pool')]/text()").extract())      
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        furnished = "".join(response.xpath("//div[@class='nfw-span9 house-info']/div/text()[contains(.,'furnished') or contains(.,'Furnished')]").extract())      
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished:
                item_loader.add_value("furnished", True)

        pets_allowed = "".join(response.xpath("//ul/li[contains(.,'Pets ')]/text()").extract())
        if pets_allowed:
            if "approval" in pets_allowed.lower() :
                item_loader.add_value("pets_allowed", True)

        parking = "".join(response.xpath("//div[@class='housing-list info-list']/span[@class='garage']//text()").extract())
        if parking:
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))


        item_loader.add_xpath("landlord_name", "//ul[@class='team-list']/li/a[2]/text()")
        item_loader.add_xpath("landlord_phone", "//ul[@class='team-list']/li/a[3]/text()")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None