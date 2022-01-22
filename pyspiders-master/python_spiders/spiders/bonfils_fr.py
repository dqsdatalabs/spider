# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re

class MySpider(Spider):
    name = 'bonfils_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "https://www.bonfils.fr/wp-admin/admin-ajax.php"
    headers = {
        'authority': 'www.bonfils.fr',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'origin': 'https://www.bonfils.fr',
        'referer': 'https://www.bonfils.fr/',
        'accept-language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        start_urls = [
            {
                "formdata" : {
                    'action': 'uwpqsf_ajax',
                    'getdata': 'unonce=eddba95ddd&uformid=19020&taxo%5B0%5D%5Bname%5D=property_city&taxo%5B0%5D%5Bopt%5D=1&taxo%5B1%5D%5Bname%5D=property_action_category&taxo%5B1%5D%5Bopt%5D=1&taxo%5B1%5D%5Bterm%5D=location&taxo%5B2%5D%5Bname%5D=property_category&taxo%5B2%5D%5Bopt%5D=1&taxo%5B2%5D%5Bterm%5D%5B%5D=appartement&cmf%5B0%5D%5Bmetakey%5D=property_price&cmf%5B0%5D%5Bcompare%5D=4&cmf%5B0%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B1%5D%5Bmetakey%5D=property_price&cmf%5B1%5D%5Bcompare%5D=6&cmf%5B1%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B2%5D%5Bmetakey%5D=property_price&cmf%5B2%5D%5Bcompare%5D=4&cmf%5B2%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B3%5D%5Bmetakey%5D=property_price&cmf%5B3%5D%5Bcompare%5D=6&cmf%5B3%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B4%5D%5Bmetakey%5D=property_rooms&cmf%5B4%5D%5Bcompare%5D=11&cmf%5B4%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B5%5D%5Bmetakey%5D=property_size&cmf%5B5%5D%5Bcompare%5D=4&cmf%5B5%5D%5Bvalue%5D=uwpqsfcmfall',
                    'pagenum': '1'
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'action': 'uwpqsf_ajax',
                    'getdata': 'unonce=eddba95ddd&uformid=19020&taxo%5B0%5D%5Bname%5D=property_city&taxo%5B0%5D%5Bopt%5D=1&taxo%5B1%5D%5Bname%5D=property_action_category&taxo%5B1%5D%5Bopt%5D=1&taxo%5B1%5D%5Bterm%5D=location&taxo%5B2%5D%5Bname%5D=property_category&taxo%5B2%5D%5Bopt%5D=1&taxo%5B2%5D%5Bterm%5D%5B%5D=maison&cmf%5B0%5D%5Bmetakey%5D=property_price&cmf%5B0%5D%5Bcompare%5D=4&cmf%5B0%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B1%5D%5Bmetakey%5D=property_price&cmf%5B1%5D%5Bcompare%5D=6&cmf%5B1%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B2%5D%5Bmetakey%5D=property_price&cmf%5B2%5D%5Bcompare%5D=4&cmf%5B2%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B3%5D%5Bmetakey%5D=property_price&cmf%5B3%5D%5Bcompare%5D=6&cmf%5B3%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B4%5D%5Bmetakey%5D=property_rooms&cmf%5B4%5D%5Bcompare%5D=11&cmf%5B4%5D%5Bvalue%5D=uwpqsfcmfall&cmf%5B5%5D%5Bmetakey%5D=property_size&cmf%5B5%5D%5Bcompare%5D=4&cmf%5B5%5D%5Bvalue%5D=uwpqsfcmfall',
                    'pagenum': '1'
                    },
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            yield FormRequest(self.url,
                         callback=self.parse,
                         headers=self.headers,
                         formdata=item["formdata"],
                         dont_filter=True,
                         meta={'property_type': item.get('property_type'), 'formdata': item['formdata']})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//h4/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            formdata = response.meta["formdata"]
            formdata["pagenum"] = str(page)
            yield FormRequest(self.url,
                         callback=self.parse,
                         headers=self.headers,
                         formdata=formdata,
                         dont_filter=True,
                         meta={"property_type": response.meta["property_type"], "formdata": formdata, "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bonfils_PySpider_france")

        external_id = response.xpath("//div[contains(@class,'reference')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'ville')]//text()").get()
        if address:
            zipcode = address.split(" ")[-1].replace("(","").replace(")","")
            city = address.split(zipcode)[0].replace("(","")
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//div[contains(@class,'details')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            if "garantie"in desc:
                deposit = desc.split("garantie")[1].split("de ")[1].split(" ")[0]
                item_loader.add_value("deposit", deposit)
            if "de provision sur charges" in desc:
                utilities = desc.split("de provision sur charge")[0].split("avec")[-1].strip().split(" ")[0]
                item_loader.add_value("utilities", utilities)
        
        rent = response.xpath("//div[contains(@class,'prix')]/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//span[contains(@class,'mc-property_bedrooms')]//text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(@class,'mc-property_rooms')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//div[contains(@class,'listing_detail info-generales')][contains(.,'Salle')]//div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//span[contains(@class,'mc-property_size')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters)))

        energy_label = response.xpath("//div[contains(@class,'dpe')]//img//@src").get()
        if energy_label:
            energy_label = energy_label.split("dpe-")[1].split(".")[0]
            if "no" not in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        balcony_terrace = response.xpath("//i[contains(@class,'mc-balcon-terrasse')]//parent::div//text()").get()
        if balcony_terrace:
            item_loader.add_value("balcony", True)
            item_loader.add_value("terrace", True)

        images = [x for x in response.xpath("//a[contains(@class,'prettygalery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("//div[contains(@class,'agent_details')]//h3//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[contains(@class,'agent_detail')]//a[contains(@href,'tel')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//div[contains(@class,'agent_detail')]//a[contains(@href,'mailto')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()