# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import json
from ..loaders import ListingLoader

class FranklynjamesSpider(scrapy.Spider):
    name = "franklynjames"
    allowed_domains = ["franklynjames.co.uk"]
    start_urls = 'https://www.franklynjames.co.uk/search/?type=rent'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    external_source = "Franklynjames_PySpider_united_kingdom_en"
    def start_requests(self):
        yield scrapy.Request(self.start_urls, self.jump)    
    
    def jump(self, response):
        script = response.xpath("//script[contains(.,'xagentNonce')]/text()").get()
        if script:
            xagentNonce = script.split("xagentNonce = '")[1].split("'")[0].strip()
            start_urls = [
                {'url': f"https://www.franklynjames.co.uk/wp-admin/admin-ajax.php?price_min=200&price_max=100000000000&sortby=desc&priority=To+Let&department=Residential+Lettings&action=xagent_get_properties&nonce={xagentNonce}", 'property_type': ''},
            ]
            for url in start_urls:
                yield scrapy.Request(
                    url=url.get('url'),
                    callback=self.parse, 
                    meta={'property_type': url.get('property_type')},
                    dont_filter=True
                )
            

    def parse(self, response, **kwargs):
        # parse details of the pro
        page = response.meta.get('page', 12)
        seen = False
        data_json = json.loads(response.text)
        for data in data_json:

            seen = True
            item_loader = ListingLoader(response=response)
            desc = data["bullet5"]
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else: 
                desc = data["main_advert"]
                if get_p_type_string(desc):
                    item_loader.add_value("property_type", get_p_type_string(desc))
                else: 
                    return
            external_link = data['link']
            external_id = str(data['reference'])
            # city = data['area']
            city = ""
            zipcode = data['postcode']
            address = data['advert_heading']
            if address:
                city = address.split(",")[-2]
            try:
                room_count = data['bedrooms']
            except:
                room_count = ''
            try:
                bathrooms = data['bathrooms']
            except:
                bathrooms = ''
            lat = data['latitude']
            lon = data['longitude']  
            rent_string = data['price_text']
            description = data['main_advert']
            position = data['position']
            imageUrl = []
            images = data['pictures']
            for img in images:
                imgurl = img['url']
                imageUrl.append(imgurl)
            title = data['advert_heading']
            furnished = data['bullet1']
            
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('title', title)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('description', description)
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_value('currency', "GBP")
            item_loader.add_value('images', imageUrl)
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('position', position)
            item_loader.add_value('longitude', str(lon))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            if furnished:
                item_loader.add_value('furnished', True)
            item_loader.add_value('landlord_name', 'Franklyn James')
            item_loader.add_value('landlord_email', 'docklands@franklynjames.co.uk')
            item_loader.add_value('landlord_phone', '020 7005 6080')
            item_loader.add_value("external_source", self.external_source)
            yield item_loader.load_item() 
       
        # if page == 12 or seen:
        #     f_url = response.url.replace(f"start={page-12}", f"start={page}")
        #     yield scrapy.Request(f_url, callback=self.parse, meta={"page": page+12, "property_type": response.meta.get('property_type')})

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None