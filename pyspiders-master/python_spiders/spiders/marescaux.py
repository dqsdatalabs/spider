# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..helper import currency_parser, extract_number_only, remove_white_spaces, remove_unicode_char,\
    property_type_lookup, format_date
from ..items import ListingItem
from python_spiders.loaders import ListingLoader

class MarescauxSpider(scrapy.Spider): 
    name = 'marescaux'
    allowed_domains = ['marescaux.be']
    start_urls = ['https://www.marescaux.be/filter-huur?field_location_tid=All&field_type_tid=53&field_bedrooms_value=All&field_bathrooms_value=All&field_price_value%5Bmin%5D=0&field_price_value%5Bmax%5D=5000',
                  'https://www.marescaux.be/filter-huur?field_location_tid=All&field_type_tid=4&field_bedrooms_value=All&field_bathrooms_value=All&field_price_value%5Bmin%5D=0&field_price_value%5Bmax%5D=5000',
                  'https://www.marescaux.be/filter-huur?field_location_tid=All&field_type_tid=3&field_bedrooms_value=All&field_bathrooms_value=All&field_price_value%5Bmin%5D=0&field_price_value%5Bmax%5D=5000']
    position = 0
    execution_type = 'testing'
    country = 'belgium' 
    locale ='fr'
    external_source="Marescaux_PySpider_belgium_fr"

    def parse(self, response, **kwargs):
        listings = response.xpath(".//div[@class='span9']//div[contains(@class, 'span3')]//h2/a/@href")
        for property_url in listings:
            yield scrapy.Request(
                url=response.urljoin(property_url.extract()),
                callback=self.get_details
            )
        next_page_url = response.xpath(".//li[@class='pager-next']//a/@href").extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse
            )

    def get_details(self, response):
        item = ListingItem()
        item_loader = ListingLoader(response=response)
        self.position += 1
        external_link = response.url
        title = ''.join(response.xpath(".//h1/text()").extract())
        images = response.xpath(".//ul[@class='slides']/li/@data-thumb").extract()
        description = ''.join(response.xpath(".//div[contains(@class, 'field-type-text-with-summary')]\
            //text()").extract())
        rent = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-price')]\
            //div[@class='field-items']//text()").extract())

        city = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-location')]\
            //div[@class='field-items']//text()").extract())
        address = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-adres')]\
            //div[@class='field-items']//text()").extract())
        room_count = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-bedrooms')]\
            //div[@class='field-items']//text()").extract())
        square_meters = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-bewoonbare-oppervlakte')\
            ]//div[contains(@class, 'field-item')]/text()").extract())
        available_date = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-beschikbaar-op')]\
            //div[contains(@class, 'field-item')]/text()").extract())
        kitchen_amenities = ''.join(response.xpath(".//div[contains(@class, 'field-name-field-general-amenities')]\
            //div[contains(@class, 'field-item')]/text()").extract())
        terrace_amenities = ''.join(response.xpath(
            ".//div[contains(@class, 'field-name-field-kenmerken-tuin-en-terras')]"
            "//div[contains(@class, 'field-item')]/text()").extract())

        utilities = "".join(response.xpath("//div[@class='span3']/div/div[div[contains(.,'Onkosten:')]]/div[2]//text()").extract())
        if utilities:
            item['utilities'] = utilities.split(" ")[1].strip()

        property_type=response.xpath("//div[@class='field-label'][contains(.,'Type')]/following-sibling::div/div/a/text()").get()
        if property_type:
            if "appartement" in property_type.lower():
                item["property_type"]="apartment"
            elif "villa" in property_type.lower():
                item["property_type"]="house"
            elif "woning" in property_type.lower():
                item["property_type"]="house"
            else:
                return
        else:
            return

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            print(external_id.split("node/")[-1])
            item["external_id"] = external_id.split("node/")[-1]

        landlord_name = 'Valentine Marescaux'
        landlord_email = 'valentine@marescaux.be' 
        landlord_phone = '+32 476 29 00 81'

        item["zipcode"] = response.xpath("//script[contains(.,'postalCode')]/text()").get().split('postalCode": "')[1].split('"')[0].strip() if response.xpath("//script[contains(.,'postalCode')]/text()").get() else None
        item['external_source'] = self.external_source
        item['external_link'] = external_link
        item['title'] = title
        item['images'] = images
        item['description'] = remove_white_spaces(description)
        if rent:
            item['rent'] = remove_unicode_char(''.join(rent.split(' ')))
            item['currency'] = "EUR"
        
        item['city'] = city
        item['address'] = address
        if room_count:
            item['room_count'] = room_count
        item['square_meters'] = extract_number_only(remove_unicode_char(square_meters))
        item['position'] = self.position
        item['landlord_name'] = landlord_name
        item['landlord_email'] = landlord_email
        item['landlord_phone'] = landlord_phone

        bathroom_count = "".join(response.xpath("//div[@class='span3']/div/div[div[contains(.,'Badkamers:')]]/div[2]//text()").extract())
        if bathroom_count:
            bathroom = bathroom_count.strip()
            item['bathroom_count'] = int(bathroom)

        if available_date:
            item['available_date'] = format_date(available_date, "%d.%m.%Y")
        if 'Vaatwasser' in kitchen_amenities:
            item['dishwasher'] = True
        if 'Terras' in terrace_amenities:
            item['terrace'] = True
        if item.get('rent', None):
            item['rent'] = int(item['rent']) if item['rent'].isdigit() else None
        if item.get('room_count', None):
            item['room_count'] = int(item['room_count']) if item['room_count'].isdigit() else None
        if item.get('square_meters', None):
            item['square_meters'] = int(item['square_meters']) if item['square_meters'].isdigit() else None
        yield item
