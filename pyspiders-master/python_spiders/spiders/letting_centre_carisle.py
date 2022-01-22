# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy 
from ..loaders import ListingLoader 


class LettingCentreCarisleSpider(scrapy.Spider):
    name = 'letting_centre_carisle'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    start_urls = ["https://www.lettingcentrecarlisle.co.uk/properties-to-let?start=0"]
    current_page = 1

    def parse(self, response):
        # the commented line loops through the whole card while the loop uncommented loops through only the urls
        # for listing in response.css(".columnProps>#smallProps>div"):
        for listing in [response.urljoin(i) for i in response.css(".columnProps>#smallProps>div p>a::attr(href)").extract()]:
            yield scrapy.Request(listing, callback=self.get_info)
        
        self.current_page += 1
        if self.current_page <= int(re.search(r"\d+$", response.css(".pagination .pull-right::text").get()).group()):
            yield scrapy.Request(re.sub(r"[\d]+$", str((self.current_page - 1) * 12), self.start_urls[0]), callback=self.parse)

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", f"{''.join(self.name.split('_'))}_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("title", response.css("title::text").get())
        item_loader.add_xpath("external_id", "substring-after(//div[b[.='Ref #']]/text(),': ')")
        rented = response.xpath("//div[b[.='Sale Type']]/text()[contains(.,'Let STC')]").get()
        if rented:
            return
        description =  response.css(".span12>p::text").get()
        item_loader.add_value("description", description.strip())
        address = response.css("h1::text").get().strip()
        item_loader.add_value("city", address.split(",")[-1].strip())
        zipcode = response.xpath("//div[@class='eapow-sidecol eapow-mainaddress']/address/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", " ".join(zipcode.split(" ")[-2:]).strip())

        item_loader.add_value("address", address)
        for feature in response.css("#starItem>li::text").extract():
            feature = feature.lower()
            # if "house" in feature:
            #     item_loader.add_value("property_type", "house")
            # elif "apartment" in feature or "flat" in feature:
            #     item_loader.add_value("property_type", "apartment")
            if "garage" in feature:
                item_loader.add_value("parking", True)
            elif "epc" in feature:
                item_loader.add_value("energy_label", re.search(r"(?<=')[a-z](?=')", "energy efficient. epc rating: 'b'").group())
        property_type =" ".join(response.xpath("//ul[@id='starItem']/li//text()").getall())
        if property_type:
            if get_p_type_string(property_type): 
                item_loader.add_value("property_type", get_p_type_string(property_type))
            else: 
                property_type = "".join(response.xpath("//div[@class='span12 eapow-desc-wrapper']/p//text()").getall())
                if get_p_type_string(property_type): 
                    item_loader.add_value("property_type", get_p_type_string(property_type))
        propertycheck=item_loader.get_output_value("property_type")
        if not propertycheck:
            item_loader.add_value("property_type","house")
        latitude=response.xpath("//script[contains(.,'lon')]/text()").get()
        if latitude:
            latitude=latitude.split("lon")[1].split(",")[0]
            item_loader.add_value("latitude",latitude.replace('"',"").replace("-",""))
        longitude=response.xpath("//script[contains(.,'lon')]/text()").get()
        if longitude:
            longitude=longitude.split("lat")[1].split(",")[0]
            item_loader.add_value("longitude",longitude.replace('"',"").replace("-",""))
        
       
        beds = response.css(".propertyIcon-bedrooms+span::text").get()
        if beds:
            item_loader.add_value("room_count", int(beds))
        baths = response.css(".propertyIcon-bathrooms+span::text").get()
        if baths:
            item_loader.add_value("bathroom_count", int(baths))
        images = response.css("#slider img::attr(src)").extract()
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        rent = response.css(".eapow-detail-price::text").get()
        if rent:
            if "weekly" in rent.lower() or "pw" in rent.lower():
                rent = "".join(filter(str.isnumeric, rent.split('.')[0].replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent)*4)))
            else:
                item_loader.add_value("rent", re.sub(r"[^\d]", "",rent.split('.')[0].split("-")[0] ))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", [i for i in response.css(".span10 b::text").extract() if ":" not in i][0])
        item_loader.add_value("landlord_phone", "01228 819333")
        item_loader.add_value("landlord_email", "hello@lettingcentrecarlisle.co.uk")
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None