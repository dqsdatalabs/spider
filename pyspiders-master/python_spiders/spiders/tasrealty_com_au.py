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

class MySpider(Spider):
    name = 'tasrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'tasrealty.com.au',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 YaBrowser/20.12.3.140 Yowser/2.5 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://tasrealty.com.au/for-sale-rent/Default.aspx',
        'accept-language': 'tr,en;q=0.9',
        'cookie': '__utma=107203591.96915058.1612347813.1612347813.1612347813.1; __utmc=107203591; __utmz=107203591.1612347813.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; __utmb=107203591.5.10.1612347813'
    }

    def start_requests(self):
        start_url = "https://tasrealty.com.au/for-rent/Default.aspx"
        yield Request(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing']/div"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            property_type = "".join(item.xpath(".//div[@class='listing_content']/p//text()").getall())
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
                else: yield Request(follow_url, callback=self.populate_item)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Tasrealty_Com_PySpider_australia")    
        item_loader.add_xpath("title","//h3/span/text()")
        item_loader.add_xpath("external_id", "//p[contains(.,'Property Code:')]/span/text()")
        city =response.xpath("//div[@class='details']/h4/span/text()").get()
        if city:
            item_loader.add_value("city", city.strip() )
        address =", ".join(response.xpath("//div[@class='details']/h4/following-sibling::p[1]/span/text() | //div[@class='details']/h4/span/text()").getall())
        if address:
            item_loader.add_value("address", address )

        item_loader.add_xpath("room_count", "//div[@class='bedrooms']/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='bathrooms']/text()")
        item_loader.add_value("currency","AUD")
        rent = "".join(response.xpath("//p[@id='price']/span/text()").getall())
        if rent:
            rent = rent.lower().split("$")[-1].split("p")[0].strip()
            item_loader.add_value("rent", str(int(float(rent)) * 4))

        zipcode = response.xpath("//div[@class='details']//a/@href[contains(.,'maps?hl')]").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("+")[-1])
        available_date = response.xpath("//p/span[@id='RepeaterResult2_Label4_0']//text()[contains(.,'AVAILABLE')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("AVAILABLE")[1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//p/span[@id='RepeaterResult2_Label4_0']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        if response.meta.get("property_type"):
            item_loader.add_value("property_type", response.meta["property_type"])
        elif get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return 
        
        images = [x for x in response.xpath("//div[@id='slider']//ul[@class='slides']/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//ul[@id='f_list']/li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        parking = response.xpath("//div[@class='carspaces']/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//ul[@id='f_list']/li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        landlord_name = response.xpath("//div[span[.='Contact Agent: ']]/following-sibling::div[2]/p/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "TAS REALTY")
        landlord_phone = response.xpath("//div[span[.='Contact Agent: ']]/following-sibling::div[2]//span[@id='txtMobile']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "+612 9953 7288")
      
        item_loader.add_xpath("landlord_email", "substring-after(//div[span[.='Contact Agent: ']]/following-sibling::div[2]//a[@id='HrefcontactAgent']/@href,'mailto:')")

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