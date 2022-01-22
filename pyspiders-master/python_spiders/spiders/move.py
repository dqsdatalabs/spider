# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
import dateparser
def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[-1]
    city = zip_city.split(" ")[0]
    zipcode = zip_city.replace(city, '')
    return city, zipcode

class MoveSpider(scrapy.Spider):
    name = "move"
    allowed_domains = ["move.uk.net"]
    start_urls = (
        'http://www.move.uk.net/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        for page in range(1, 8):
            start_urls = "https://move.uk.net/page/{}/?ct_ct_status=available-to-let&ct_city&search-listings=true&ct_orderby=priceDESC".format(page)
            yield scrapy.Request(
                url=start_urls,
                callback=self.parse, 
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="listings-results"]//h5/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)

    def get_property_details(self, response):
        property_site_type = response.xpath('//li[contains(@class, "property-type")]/span/following-sibling::span/text()').extract_first('')
        if 'house' in property_site_type.lower() :
            property_type = 'house'
        elif 'apartment' in property_site_type.lower():
            property_type = 'apartment'
        elif 'flat' in property_site_type.lower():
            property_type = 'apartment'
        elif 'terraced' in property_site_type.lower():
            property_type = 'house'
        else:
            property_type = ''
        if property_type:
        
            title = response.xpath('//h1[@id="listing-title"]/text()').extract_first('')
            external_link = response.url
            address = response.xpath('//p[contains(@class, "location")]/text()').extract_first('').strip()
            city, zipcode = extract_city_zipcode(address) 
            room_count = response.xpath('//li[contains(@class, "bed")]/span[2]/text()').extract_first('')
            bathrooms = response.xpath('//li[contains(@class, "bath")]/span[2]/text()').extract_first('') 
            rent_string = response.xpath('//span[@class="listing-price"]/text()').extract_first('').strip()
            floor_plan_img = response.xpath('//div[@id="listing-plans"]/img/@src').extract_first('').strip()
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('title', title)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode.strip())
            item_loader.add_xpath('description', '//div[@id="listing-content"]/p//text()')
            item_loader.add_value('rent_string', rent_string)
            images = [x for x in response.xpath("//a[@class='gallery-item']/@href").getall()]
            if images:
                item_loader.add_value("images", images)
            if floor_plan_img:
                item_loader.add_value('floor_plan_images', floor_plan_img)
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished') ]//text()").extract_first()
            if furnished:
                if "unfurnished" in furnished.lower():
                    item_loader.add_value('furnished', False)
                else:
                    item_loader.add_value('furnished', True)

            available_date="".join(response.xpath("//ul[@class='propfeatures col span_6']/li/text()[contains(.,'*') and contains(.,'Available')]").getall())
            if available_date:
                date2 =  available_date.split("Available")[1].replace("*","").strip()
                if date2:

                    date_parsed = dateparser.parse(
                        date2, date_formats=["%m-%d-%Y"]
                    )
                    if date_parsed:
                        date3 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date3)

            parking = response.xpath("//li[contains(.,'Parking ')]//text()").extract_first()
            if parking:
                item_loader.add_value('parking', True)

            item_loader.add_value('landlord_name', 'Move Sales & Lettings')
            item_loader.add_value('landlord_email', 'info@move.uk.net')
            item_loader.add_value('landlord_phone', '01242 257333')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item() 
