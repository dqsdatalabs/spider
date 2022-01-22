# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy

from ..helper import extract_number_only
from ..loaders import ListingLoader


class ResidentiallandComPyspiderUnitedkingdomEnSpider(scrapy.Spider):
    name = 'residentialland_com'
    allowed_domains = ['residentialland.com']
    start_urls = ['https://www.residentialland.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        self.position = 0
        start_urls = ['https://www.residentialland.com/rental-search/?rlx=y']
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//h3[@class="results-title"]//a/@href').extract()
        if listings:
            for property_item in listings:
                # some url redirect to other url instead of overview page
                detail_url = f"https://www.residentialland.com{property_item.replace('/properties/', '/overview/')}"
                yield scrapy.Request(
                    url=detail_url,
                    callback=self.get_property_details,
                    meta={'request_url': detail_url,}
                )

        next_page_url = response.xpath('.//li[contains(@id,"next")]/a/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(f"https://www.residentialland.com{next_page_url}"),
                callback=self.parse,
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split("/")[-2])

        # location details , lat and long can't be extract as they are hardcoded with refid instead of coordinates like https://ew-maps.novaloca.com/default.aspx?key=sdXQRSRGPAJQQDMqf4DlKzVOudAxwLC5ArMEOFDa7OteXhnWY3O3KQivqX9eJr26XCdgrFxkNdJHiqsY9NmaKYeT1kd44Yzsaa3+TOfJQpCKXIS+9CXoLUZUcU87dXN8mDfm+2iZuwxHJ6/wvuqekzHZwYbkLML4cM4FSjwH2CopT5RYVcgBApg==&propertyref=1066
        item_loader.add_xpath('address', './/h1[@id="ctl00_ctl00_maincontent_h1Name"]/text()')
        item_loader.add_xpath('zipcode', './/span[@itemprop="postalCode"]/text()')
        item_loader.add_xpath('city', './/span[@itemprop="addressLocality"]/text()')

        item_loader.add_xpath('title', './/title/text()')

        description = " ".join(response.xpath('.//p[contains(@id,"Description")]/text()').extract())
        if description is None:
            description = response.xpath('.//span[@class="availablepropertydescription"]/text()').extract()

        rents = [int(extract_number_only(rent, thousand_separator=',')) for rent in
                 response.xpath('.//h3[contains(@id,"Price")]/strong').extract()]

        if len(rents) > 1:
            item_loader.add_value('rent_string', f"£{min(rents) * 4}")
            item_loader.add_value('description',
                                  f"{description} Note: price for this property range from £{min(rents) * 4} to £{max(rents) * 4}")
        elif rents:
            item_loader.add_value('rent_string', f"£{rents[0] * 4}")
            item_loader.add_value('description', description)

        item_loader.add_xpath('images', './/ul[@id="pikame"]//img/@src')

        square_meters = response.xpath("//li/p[contains(.,'sq')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("sq")[0].strip().split(" ")[-1]
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        room_count = response.xpath("//h5[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])

        # landlord details
        item_loader.add_xpath('landlord_name', './/div[@class="contact-info"]/img/@alt')
        item_loader.add_value('landlord_email', 'enquiries@residentialland.com')
        # specify explicitly as many contact numbers are present
        landlord_phone = response.xpath('.//div[@class="contact-info"]//a[@class="tel"]/text()').extract_first()
        item_loader.add_value('landlord_phone', landlord_phone)

        parking = response.xpath('.//li[@class="parking"]')
        if parking:
            item_loader.add_value('parking', True)

        washing = response.xpath('.//li[@class="laundry"]')
        if washing:
            item_loader.add_value('washing_machine', True)

        washing = response.xpath('.//li[@class="lifts"]')
        if washing:
            item_loader.add_value('elevator', True)

        pool = response.xpath('.//li[@class="pool"]')
        if pool:
            item_loader.add_value('swimming_pool', True)

        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: print("----", response.url)

        self.position += 1
        item_loader.add_value('position', self.position)

        item_loader.add_value("external_source",
                              "ResidentiallandCom_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "end of terrace" in p_type_string.lower()):
        return "house"
    else:
        return None