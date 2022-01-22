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
    name = 'zoopla_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    external_source='Zoopla_PySpider_united_kingdom'
    custom_setting={
        "HTTPCACHE_ENABLED": False
    }

    def start_requests(self):
        start_urls = [
            {
                "url": ["https://www.zoopla.co.uk/to-rent/property/uk/"
                ],
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )

    # 1. FOLLOWING 
    def parse(self, response):
        page = response.meta.get('page', 2)       
        seen = False
        for item in response.xpath("//a[@data-testid='listing-details-image-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"page": page+1})
            seen = True
        if page==2 or seen:
            url = f"https://www.zoopla.co.uk/to-rent/property/uk/?page_size=25&price_frequency=per_month&q=UK&radius=0&results_sort=newest_listings&pn={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})
 


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Zoopla_PySpider_united_kingdom")
        propertype = "".join(response.xpath("//span[@data-testid='title-label']/text()").getall())
        if get_p_type_string(propertype):
            item_loader.add_value("property_type", get_p_type_string(propertype))
        else:
            return

        item_loader.add_value("external_id", response.url.split("/")[-2])
        title =response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)
        desc = "".join(response.xpath("//p[@class='description-text']/following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)
        address =response.xpath("//span[@data-testid='address-label']/text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(",")[-1]
            try:
                city = city.split(" ")[:-1]
                if len(city)==1:
                    city = city[0].strip(" ")
                else:
                    city = " ".join(city).strip(" ")
            except:
                city = city[0].strip(" ")
            item_loader.add_value("city", city.strip())
            zipcode=address.split(",")[-1].strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode.strip())
        desc = "".join(response.xpath("//div[@data-testid='listing_description']//span//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)
        rent =response.xpath("//span[@data-testid='price']/text()").get()
        if rent:
            rent = rent.strip().split("£")[1].split("p")[0].replace(",","").strip()
            item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "GBP")
        room_count =response.xpath("//span[@data-testid='beds-label']/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)
        bathroom_count =response.xpath("//span[@data-testid='baths-label']/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        images = [x for x in response.xpath("//div[@data-testid='gallery']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//span[@data-testid='availability']/text()").get()
        if available_date:
            if not "now" in available_date.lower():
                available_date=available_date.split("from")[-1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        liste=response.xpath("//ul[@data-testid='listing_features_bulletted']//li//text()").getall()
        if liste:
            for i in liste:
                if "furnished" in i.lower():
                    if "unfurnished" in i.lower(): 
                        item_loader.add_value("furnished", False)
                    else:
                        item_loader.add_value("furnished", True)
        energy_label=response.xpath("//div[@data-testid='listing_description']//span//text()").getall()
        if energy_label:
            for i in energy_label:
                if "epc rating" in i.lower():
                    energy=i.split(":")[-1].split(".")[-1]
                    if len(energy)<3:
                        item_loader.add_value("energy_label",energy)
        latlng=response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
        if latlng:
            latitude=latlng.split("latitude")[-1].split(",")[0].replace('":',"")
            if latitude:
                item_loader.add_value("latitude",latitude)
            longitude=latlng.split('longitude')[-1].split("}")[0].replace("-","").replace('":',"")
            if longitude:
                item_loader.add_value("longitude",longitude)

        name=response.xpath("//div[contains(@class,'AgentName')]//p//text()").get()
        if name:
            item_loader.add_value("landlord_name", name)

        phone = response.xpath("//script[@type='application/json']//text()").get()
        if phone:
            phone="".join(phone.split('"phone":"')[1].split('"}')[0])
            item_loader.add_value("landlord_phone", phone)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None