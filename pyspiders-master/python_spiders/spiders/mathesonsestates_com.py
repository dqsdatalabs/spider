# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader

class MySpider(Spider):
    name = 'mathesonsestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='MathesonsEstates_PySpider_united_kingdom'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_url = "http://www.mathesonsestates.com/let/property-to-let/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='tab-galleryview']/div"):
            url = response.urljoin(item.xpath(".//h2/a/@href").get())
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li[@class='navarrow']/a[i[@class='fa-angle-right fa']]/@href").get()   
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)  

        title= response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        external_id = "".join(response.xpath("//a[@class='print-button tab_control smallcaps tab_allowDefault']//@href").extract())
        if external_id:
            ex_id = external_id.split("/print-details")[0]
            external_id = ex_id.split("/property/")[-1]
            item_loader.add_value("external_id",external_id)

        f_text = " ".join(response.xpath("//div[@id='propertyInfo_desc']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//h2[@class='details_h2']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return              
        address =response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
        room_count = response.xpath("//h2/text()[contains(.,'bedroom')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("bedroom")[0].strip().split("|")[-1])
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("//h2/text()[contains(.,'bathroo')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bathroo")[0].strip().split(",")[-1])
       
        rent = response.xpath("//span[@class='nativecurrencyvalue']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.split(".")[0])
        item_loader.add_value("currency", "GBP")

        description = " ".join(response.xpath("//div[@id='propertyInfo_desc']//text()[.!='About this property']").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@class='mygallery']//li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        furnished = response.xpath("//li[span[contains(.,'FURNISHED')]]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            else:
                item_loader.add_value("furnished",True)
        item_loader.add_value("landlord_phone", "020 8965 2250")
        item_loader.add_value("landlord_email", "harlesden@mathesonsestates.com")
        item_loader.add_value("landlord_name", "Harlesden Sales and Lettings")

        terrace = response.xpath("//li[span[contains(.,'terrace')]]//text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
  
        balcony = response.xpath("//li[span[contains(.,'balcony')]]//text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        parking = response.xpath("//li[span[contains(.,'parking')]]//text()").get()
        if parking:
            item_loader.add_value("parking",True)
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "maisonet" in p_type_string.lower() or "home " in p_type_string.lower()):
        return "house"    
    else:
        return None