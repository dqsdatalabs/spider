import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters

class SunrealgroupSpider(scrapy.Spider):
    name = 'sunrealgroup'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['sunrealgroup.com']
    start_urls = ['https://sunrealgroup.com/?s=&mkdf-property-search=yes']

    def parse(self, response):
        for page in response.css(".mkdf-pli-link.mkdf-block-drag-link::attr(href)").getall():
            yield scrapy.Request(url=page, callback=self.parse_page)
        
    def parse_page(self, response):
        apartments = ['condo','apartment','unit']
        flag = False
        images          = response.css(".mkdf-property-single-lightbox img::attr(src)").getall()
        rent            = int(response.css(".mkdf-property-price span .mkdf-property-price-value::text").get().replace(",",""))
        square_meters   = response.css(".mkdf-property-size-value::text").get()
        room_count      = response.css(".mkdf-property-content span.mkdf-property-value::text").re("[0-9A-Za-z]+\W*[Bb]edroom")
        bathroom_count  = response.css(".mkdf-property-content span.mkdf-property-value::text").re("[0-9]+\W*Bathrooms")
        
        title           = response.css(".mkdf-property-title-left h2::text").get().strip()
        landlord_email  = "".join(response.css(".mkdf-contact-label a::text").getall()).strip()
        description     = fix_desc(remove_white_spaces("".join(response.css(".mkdf-property-description-items p::text").getall())))
        address         = response.css(".mkdf-full-address.mkdf-label-items-item .mkdf-label-items-value::text").get().strip().split(",")        
        zipcode         = response.css(".mkdf-zip-code .mkdf-label-items-value::text").get().strip()
        property_type   = response.css(".mkdf-property-title-left h2::text").get().strip()
        external_id     = response.css(".mkdf-property-id-value::text").get().strip()
        address, city   = address[0], address[1].strip()

        if room_count:
            if 'bachelor' in room_count[0].lower():
                room_count = 1
            else:
                room_count      = int(room_count[0].lower().replace("bedrooms","").replace("bedroom","").strip())
        if bathroom_count:
            bathroom_count  = int(bathroom_count[0].lower().replace("bathrooms","").strip())
        
        for apart in apartments:
            if apart in property_type.lower():
                property_type = 'apartment'
                flag = True
        parking, washing_machine = '',''
        if not flag:
            property_type,parking, washing_machine = fetch_prop(response.css(".mkdf-feature.mkdf-feature-inactive .mkdf-feature-name::text").getall())
                
        if not property_type:
            property_type = 'apartment'
        if square_meters:
            square_meters = sq_feet_to_meters(int(square_meters.replace("sq ft","").strip()))
        
        item = ListingLoader(response=response)
        
        item.add_value("external_source"        ,self.external_source)
        item.add_value("address"                ,address)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"CAD")
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,images)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("property_type"          ,property_type)
        item.add_value("parking"                ,parking)
        item.add_value("washing_machine"        ,washing_machine)
        item.add_value("description"            ,description)
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("city"                   ,city)
        item.add_value("external_id"            ,external_id)

        yield item.load_item()
        
def fetch_prop(vals):
    property_type,parking, washing_machine = '','',''
    if len(vals) == 0:
        return "",'',''
    for val in vals:
        if 'condo' in val.strip().lower():
            property_type = 'apartment'
        elif 'parking' in val.strip().lower():
            parking = True
        elif 'laundry' in val.strip().lower():
            washing_machine = True
            
    return property_type,parking, washing_machine 


def fix_desc(desc):
    return desc.replace("www.alexanderplace.ca","").replace("www.sunrealgroup.com","").replace("Sunreal Property Management Ltd.","")
