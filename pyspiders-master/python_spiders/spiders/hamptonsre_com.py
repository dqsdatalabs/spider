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
import re


class MySpider(Spider):
    name = 'hamptonsre_com_disabled'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Hamptonsre_Com_PySpider_united_kingdom'
    start_urls = ["https://www.hamptonsre.com/rentals/the-hamptons-ny-area/single-family-home-type/single-family-townhouse-type"]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing-item__address-block')]/link/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

        page = response.xpath("//a[@aria-label='Next Page']/@href").extract_first()
        if page:
            url = response.urljoin(page)
            yield Request(url, callback=self.parse)

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        property_type = "".join(response.xpath("//dt[@class='listing-info__title'][contains(.,'Property Type')]/../dd/text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        address = "".join(response.xpath("//h1[@class='m-listing-title--heading']//text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
        
        item_loader.add_xpath("city", "//span[@class='locality city']/text()")
        zip = response.xpath("//span[@class='region']/text()").get()
        if zip:
            zip2 = response.xpath("//span[@class='postal-code']/text()").get()
            if zip2:
                zipcode = zip + " " + zip2
                item_loader.add_value("zipcode", zipcode)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("room_count", "//dt[@class='listing-info__title'][contains(.,'Bedrooms')]/../dd/text()")
        item_loader.add_xpath("bathroom_count", "//dt[@class='listing-info__title'][contains(.,'Full Baths')]/../dd/text()")
        item_loader.add_xpath("external_id", "//dt[@class='listing-info__title'][contains(.,'Web Id')]/../dd/text()")

        meters = "".join(response.xpath("//dt[@class='listing-info__title'][contains(.,'Interior')]/../dd/text()").extract())
        if meters:
            s_meters = meters.split(" ")[0].replace(",","").strip()
            sqm = str(int(float(s_meters) * 0.09290304))
            item_loader.add_value("square_meters", sqm.strip())

        lat_lng = response.xpath("//input[@class='direction-input'][1]/@value").extract_first()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split(",")[1].strip())

        images = [x for x in response.xpath("//div[@class='contents-wrapper  js-thumbnail-wrapper']//@data-image-url-format").getall()]
        if images:
            item_loader.add_value("images", images)

        rent = "".join(response.xpath("//div[contains(@class,'rental-availability__period')]/parent::div/div[1]/text()").extract())
        if rent:
            price = rent.replace(",","").replace("$","").strip()
            price = (int(price)/12)
            price = str(price)
            price = price.split('.')[0]
            item_loader.add_value("rent_string", price)
        item_loader.add_value("currency", "USD")
        desc = " ".join(response.xpath("//div[@itemprop='description']/div/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        pool = "".join(response.xpath("//div[@class='prop-description__amenities-list']/ul/li[.='Pool']//text()").extract())
        if pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='contact-card__details']/a/span/text())")
        item_loader.add_xpath("landlord_phone", "normalize-space(//div[@class='contact-card__details']//a[contains(@class,'o-phone-number')]/text())")
        # item_loader.add_value("landlord_email", "enquiries@simonebullen.com.au")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None