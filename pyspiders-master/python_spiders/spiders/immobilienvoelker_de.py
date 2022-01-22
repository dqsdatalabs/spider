# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
from math import ceil

import scrapy
from scrapy import Request, Selector
from scrapy.http import HtmlResponse

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class immobilienvoelker_deSpider(scrapy.Spider):
    name = 'immobilienvoelker_de'
    # allowed_domains = ['immobilienvoelker.de']
    start_urls = ['https://immobilienvoelker.de/wp-admin/admin-ajax.php']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    duplicates = {}
    position = 1
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'Parkh user','Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine','waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler',],
        'floor' : ['etage'],
        'bedroom': ['Schlafzimmer']
    }

    # 1. SCRAPING level 1
    def start_requests(self):
        formdatas = 'action=divi_machine_loadmore_ajax_handler&query=%7B%22post_type%22%3A%22mietangebote%22%2C%22post_status%22%3A%22publish%22%2C%22posts_per_page%22%3A10%2C%22tax_query%22%3A%7B%22relation%22%3A%22AND%22%2C%220%22%3A%7B%22taxonomy%22%3A%22mietangebote_category%22%2C%22field%22%3A%22slug%22%2C%22terms%22%3A%22aktiv%22%2C%22operator%22%3A%22IN%22%7D%7D%2C%22orderby%22%3A%22date%22%2C%22order%22%3A%22DESC%22%2C%22meta_query%22%3A%7B%22relation%22%3A%22AND%22%7D%2C%22error%22%3A%22%22%2C%22m%22%3A%22%22%2C%22p%22%3A0%2C%22post_parent%22%3A%22%22%2C%22subpost%22%3A%22%22%2C%22subpost_id%22%3A%22%22%2C%22attachment%22%3A%22%22%2C%22attachment_id%22%3A0%2C%22name%22%3A%22%22%2C%22pagename%22%3A%22%22%2C%22page_id%22%3A0%2C%22second%22%3A%22%22%2C%22minute%22%3A%22%22%2C%22hour%22%3A%22%22%2C%22day%22%3A0%2C%22monthnum%22%3A0%2C%22year%22%3A0%2C%22w%22%3A0%2C%22category_name%22%3A%22%22%2C%22tag%22%3A%22%22%2C%22cat%22%3A%22%22%2C%22tag_id%22%3A%22%22%2C%22author%22%3A%22%22%2C%22author_name%22%3A%22%22%2C%22feed%22%3A%22%22%2C%22tb%22%3A%22%22%2C%22paged%22%3A0%2C%22meta_key%22%3A%22%22%2C%22meta_value%22%3A%22%22%2C%22preview%22%3A%22%22%2C%22s%22%3A%22%22%2C%22sentence%22%3A%22%22%2C%22title%22%3A%22%22%2C%22fields%22%3A%22%22%2C%22menu_order%22%3A%22%22%2C%22embed%22%3A%22%22%2C%22category__in%22%3A%5B%5D%2C%22category__not_in%22%3A%5B%5D%2C%22category__and%22%3A%5B%5D%2C%22post__in%22%3A%5B%5D%2C%22post__not_in%22%3A%5B%5D%2C%22post_name__in%22%3A%5B%5D%2C%22tag__in%22%3A%5B%5D%2C%22tag__not_in%22%3A%5B%5D%2C%22tag__and%22%3A%5B%5D%2C%22tag_slug__in%22%3A%5B%5D%2C%22tag_slug__and%22%3A%5B%5D%2C%22post_parent__in%22%3A%5B%5D%2C%22post_parent__not_in%22%3A%5B%5D%2C%22author__in%22%3A%5B%5D%2C%22author__not_in%22%3A%5B%5D%2C%22ignore_sticky_posts%22%3Afalse%2C%22suppress_filters%22%3Afalse%2C%22cache_results%22%3Atrue%2C%22update_post_term_cache%22%3Atrue%2C%22lazy_load_term_meta%22%3Atrue%2C%22update_post_meta_cache%22%3Atrue%2C%22nopaging%22%3Afalse%2C%22comments_per_page%22%3A%2250%22%2C%22no_found_rows%22%3Afalse%2C%22taxonomy%22%3A%22mietangebote_category%22%2C%22term%22%3A%22aktiv%22%7D&page=0&layoutid=51910&posttype=mietangebote&noresults=51910&sortorder=date&sortasc=DESC&gridstyle=grid&columnscount=3&postnumber=1000&linklayout=on'
        header = {
            'cookie' : 'borlabs-cookie=%7B%22consents%22%3A%7B%22essential%22%3A%5B%22borlabs-cookie%22%2C%22phpsessid%22%5D%7D%2C%22domainPath%22%3A%22immobilienvoelker.de%2F%22%2C%22expires%22%3A%22Thu%2C%2014%20Jul%202022%2009%3A19%3A29%20GMT%22%2C%22uid%22%3A%22s91akj6x-hw9453ku-elh5vt3u-h397qnzw%22%2C%22version%22%3A%2211%22%7D; et-editor-available-post-50702-fb=fb',
            'Origin' : 'https://immobilienvoelker.de',
            'referer' : 'https://immobilienvoelker.de/angebote/mietangebote/',
            'content-type' : 'application/x-www-form-urlencoded; charset=UTF-8',
            'sec-fetch-site' : 'same-origin',
            'sec-fetch-mode' : 'cors',
            'sec-fetch-dest' : 'empty',
            'sec-ch-ua-platform' : '"Windows"',
            'sec-ch-ua' : '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
            'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
            'x-requested-with' : 'XMLHttpRequest',
            'accept-language' : 'en-US,en;q=0.9,ar-EG;q=0.8,ar;q=0.7'
        }
        for url in self.start_urls:
                yield scrapy.FormRequest(url, callback=self.parse, headers=header, body=formdatas, method='POST')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        parse_response = json.loads(response.body)
        html_response_string = parse_response['posts']
        html_response = Selector(text=html_response_string, type='html')
        rentals = html_response.css('.os-button-primary::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = None # response.css('::text').extract_first()
        title = response.css('h1::text').extract_first()
        description = ((((' '.join(response.css('.et_pb_text_10_tb_body p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

        list_items = response.css('.et_pb_row_3_tb_body .dmach-acf-value,.et_pb_section_3_tb_body .dmach-acf-value')
        list_items_dict = {}
        for list_item in list_items:
            hd_val = list_item.css('::text').extract()
            headers = hd_val[0]
            values = hd_val[-1]
            list_items_dict[headers] = values

        address = None
        if 'Standort' in list_items_dict.keys():
            address = list_items_dict['Standort'] +', Germany'

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        if 'Standort' in list_items_dict.keys():
            exaddress = list_items_dict['Standort'] +', Germany'
            zipcode = extract_number_only(exaddress)
        if address == zipcode:
            address = address +', '+city
        property_type = None  # response.css('::text').extract_first()
        if 'Immobilienart' in list_items_dict.keys():
            property_type = list_items_dict['Immobilienart']
            self.duplicates[property_type] = ''
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'

        if property_type == 'apartment':
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Zimmer' in list_items_dict.keys():
                room_count = list_items_dict['Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))
            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'bad' in description.lower():
                bathroom_count = 1
            available_date = None  # response.css('.availability .meta-data::text').extract_first()

            images = response.css('.et_post_gallery a::attr(href)').extract()
            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete']
                rent = int(ceil(float(extract_number_only(rent))))
            elif 'Nettomiete' in list_items_dict.keys():
                rent = list_items_dict['Nettomiete']
                rent = int(ceil(float(extract_number_only(rent))))
            deposit = None
            if 'kaution' in description.lower():
                deposit = rent*2
            utilities = None
            if 'Nebenkosten' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten']
                utilities = int(ceil(float(extract_number_only(utilities))))

            pets_allowed = None
            if 'keine haustiere' in description.lower():
                pets_allowed = False
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                parking = True

            elevator = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['terrace']):
                terrace = True

            swimming_pool = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['dishwasher']):
                dishwasher = True

            landlord_name = 'immobilienvoelker_de'
            landlord_email = 'info@immobilien-voelker.de'
            landlord_phone = "04442 706007"

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            # item_loader.add_value("external_images_count", len(images)) # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
